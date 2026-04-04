use super::traits::{Channel, ChannelMessage, SendMessage};
use async_trait::async_trait;
use uuid::Uuid;

/// Chatwoot Agent Bot channel.
///
/// This channel operates in webhook mode (push-based). Chatwoot sends
/// incoming customer messages to ZeroClaw via the Agent Bot webhook mechanism.
/// ZeroClaw processes the message through the LLM, then calls back into
/// the Chatwoot API to post a reply.
///
/// The `listen` method is a keepalive placeholder; actual message handling
/// happens in the gateway when Chatwoot sends webhook events.
pub struct ChatwootChannel {
    /// Chatwoot instance base URL (e.g. "https://chatwoot.example.com").
    base_url: String,
    /// Agent Bot access token for authenticating API callbacks.
    bot_token: String,
    client: reqwest::Client,
}

impl ChatwootChannel {
    pub fn new(base_url: String, bot_token: String) -> Self {
        // Normalize: strip trailing slash from base URL
        let base_url = base_url.trim_end_matches('/').to_string();
        Self {
            base_url,
            bot_token,
            client: crate::config::build_runtime_proxy_client("channel.chatwoot"),
        }
    }

    /// Parse an incoming Agent Bot webhook payload from Chatwoot.
    ///
    /// Chatwoot sends a JSON payload with `event`, `account`, `conversation`,
    /// and `message` (for message events). We only handle `message_created`
    /// events where the message is from a customer (incoming).
    pub fn parse_webhook_payload(&self, payload: &serde_json::Value) -> Vec<ChannelMessage> {
        let mut messages = Vec::new();

        let event = payload.get("event").and_then(|v| v.as_str()).unwrap_or("");

        // Only process incoming customer messages
        if event != "message_created" {
            tracing::debug!("Chatwoot: ignoring event type: {event}");
            return messages;
        }

        let message = match payload.get("message") {
            Some(m) => m,
            None => {
                // Some webhook formats have the message fields at the top level
                payload
            }
        };

        // Skip outgoing messages (sent by agents/bots)
        let message_type = message
            .get("message_type")
            .and_then(|v| v.as_str().or_else(|| v.as_u64().map(|_| "")).or(None))
            .unwrap_or("");

        let message_type_int = message
            .get("message_type")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        // message_type: 0 = incoming, 1 = outgoing, 2 = activity, 3 = template
        // We only handle incoming (0) messages
        if message_type == "outgoing" || message_type_int == 1 {
            tracing::debug!("Chatwoot: skipping outgoing message");
            return messages;
        }
        if message_type == "activity" || message_type_int == 2 {
            tracing::debug!("Chatwoot: skipping activity message");
            return messages;
        }
        if message_type == "template" || message_type_int == 3 {
            tracing::debug!("Chatwoot: skipping template message");
            return messages;
        }

        // Extract message content
        let content = message
            .get("content")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim();

        if content.is_empty() {
            return messages;
        }

        // Extract conversation ID for routing replies
        let conversation_id = payload
            .get("conversation")
            .and_then(|c| c.get("id"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        if conversation_id == 0 {
            tracing::warn!("Chatwoot: message has no conversation ID, skipping");
            return messages;
        }

        // Extract account ID for API path
        let account_id = payload
            .get("account")
            .and_then(|a| a.get("id"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        if account_id == 0 {
            tracing::warn!("Chatwoot: message has no account ID, skipping");
            return messages;
        }

        // Build sender identifier from contact info
        let sender_name = payload
            .get("sender")
            .or_else(|| message.get("sender"))
            .and_then(|s| {
                s.get("name")
                    .and_then(|v| v.as_str())
                    .or_else(|| s.get("email").and_then(|v| v.as_str()))
                    .or_else(|| s.get("id").and_then(|v| v.as_u64()).map(|_| ""))
            })
            .unwrap_or("unknown");

        let sender_id = payload
            .get("sender")
            .or_else(|| message.get("sender"))
            .and_then(|s| s.get("id"))
            .and_then(|v| v.as_u64())
            .map(|id| id.to_string())
            .unwrap_or_else(|| sender_name.to_string());

        // reply_target encodes account_id and conversation_id for the send() method
        let reply_target = format!("{account_id}:{conversation_id}");

        let timestamp = message
            .get("created_at")
            .and_then(|v| {
                v.as_u64().or_else(|| {
                    v.as_str().and_then(|s| {
                        chrono::DateTime::parse_from_rfc3339(s)
                            .ok()
                            .map(|dt| dt.timestamp().cast_unsigned())
                    })
                })
            })
            .unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            });

        messages.push(ChannelMessage {
            id: Uuid::new_v4().to_string(),
            sender: sender_id,
            reply_target,
            content: content.to_string(),
            channel: "chatwoot".to_string(),
            timestamp,
            thread_ts: None,
            interruption_scope_id: None,
            attachments: vec![],
        });

        messages
    }
}

#[async_trait]
impl Channel for ChatwootChannel {
    fn name(&self) -> &str {
        "chatwoot"
    }

    async fn send(&self, message: &SendMessage) -> anyhow::Result<()> {
        // recipient is "account_id:conversation_id"
        let parts: Vec<&str> = message.recipient.splitn(2, ':').collect();
        if parts.len() != 2 {
            anyhow::bail!(
                "Chatwoot: invalid recipient format '{}', expected 'account_id:conversation_id'",
                message.recipient
            );
        }
        let account_id = parts[0];
        let conversation_id = parts[1];

        let url = format!(
            "{}/api/v1/accounts/{}/conversations/{}/messages",
            self.base_url, account_id, conversation_id
        );

        let body = serde_json::json!({
            "content": message.content,
            "message_type": "outgoing",
            "private": false
        });

        let resp = self
            .client
            .post(&url)
            .header("api_access_token", &self.bot_token)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let error_body = resp.text().await.unwrap_or_default();
            let sanitized = crate::providers::sanitize_api_error(&error_body);
            tracing::error!("Chatwoot send failed: {status} — {sanitized}");
            anyhow::bail!("Chatwoot API error: {status}");
        }

        Ok(())
    }

    async fn listen(&self, _tx: tokio::sync::mpsc::Sender<ChannelMessage>) -> anyhow::Result<()> {
        // Chatwoot uses webhooks (push-based via Agent Bot), not polling.
        // Messages are received via the gateway's /chatwoot endpoint.
        tracing::info!(
            "Chatwoot channel active (webhook mode). \
            Configure Chatwoot Agent Bot outgoing_url to POST to your gateway's /chatwoot endpoint."
        );

        // Keep the task alive — it will be cancelled when the channel shuts down
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
        }
    }

    async fn health_check(&self) -> bool {
        // Verify connectivity to the Chatwoot instance
        let url = format!("{}/auth/sign_in", self.base_url);

        self.client
            .get(&url)
            .send()
            .await
            .map(|r| {
                // Any response (even 4xx) means the server is reachable
                r.status().as_u16() < 500
            })
            .unwrap_or(false)
    }

    async fn start_typing(&self, _recipient: &str) -> anyhow::Result<()> {
        // Chatwoot Agent Bot API does not support typing indicators
        Ok(())
    }

    async fn stop_typing(&self, _recipient: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_channel() -> ChatwootChannel {
        ChatwootChannel {
            base_url: "https://chatwoot.example.com".into(),
            bot_token: "test-bot-token".into(),
            client: reqwest::Client::new(),
        }
    }

    #[test]
    fn channel_name() {
        let ch = make_channel();
        assert_eq!(ch.name(), "chatwoot");
    }

    #[test]
    fn parse_incoming_message() {
        let ch = make_channel();
        let payload = serde_json::json!({
            "event": "message_created",
            "account": {"id": 1, "name": "Test Account"},
            "conversation": {"id": 42, "display_id": 100},
            "message": {
                "id": 123,
                "content": "Hello, I need help!",
                "message_type": 0,
                "created_at": 1700000000_u64
            },
            "sender": {
                "id": 99,
                "name": "John Doe",
                "email": "john@example.com"
            }
        });

        let messages = ch.parse_webhook_payload(&payload);
        assert_eq!(messages.len(), 1);
        let msg = &messages[0];
        assert_eq!(msg.content, "Hello, I need help!");
        assert_eq!(msg.reply_target, "1:42");
        assert_eq!(msg.sender, "99");
        assert_eq!(msg.channel, "chatwoot");
    }

    #[test]
    fn skip_outgoing_message_by_type_int() {
        let ch = make_channel();
        let payload = serde_json::json!({
            "event": "message_created",
            "account": {"id": 1},
            "conversation": {"id": 42},
            "message": {
                "content": "Agent reply",
                "message_type": 1
            },
            "sender": {"id": 5}
        });

        let messages = ch.parse_webhook_payload(&payload);
        assert!(messages.is_empty());
    }

    #[test]
    fn skip_outgoing_message_by_type_string() {
        let ch = make_channel();
        let payload = serde_json::json!({
            "event": "message_created",
            "account": {"id": 1},
            "conversation": {"id": 42},
            "message": {
                "content": "Agent reply",
                "message_type": "outgoing"
            },
            "sender": {"id": 5}
        });

        let messages = ch.parse_webhook_payload(&payload);
        assert!(messages.is_empty());
    }

    #[test]
    fn skip_activity_message() {
        let ch = make_channel();
        let payload = serde_json::json!({
            "event": "message_created",
            "account": {"id": 1},
            "conversation": {"id": 42},
            "message": {
                "content": "Conversation resolved",
                "message_type": 2
            },
            "sender": {"id": 5}
        });

        let messages = ch.parse_webhook_payload(&payload);
        assert!(messages.is_empty());
    }

    #[test]
    fn skip_non_message_event() {
        let ch = make_channel();
        let payload = serde_json::json!({
            "event": "conversation_resolved",
            "account": {"id": 1},
            "conversation": {"id": 42}
        });

        let messages = ch.parse_webhook_payload(&payload);
        assert!(messages.is_empty());
    }

    #[test]
    fn skip_empty_content() {
        let ch = make_channel();
        let payload = serde_json::json!({
            "event": "message_created",
            "account": {"id": 1},
            "conversation": {"id": 42},
            "message": {
                "content": "",
                "message_type": 0
            },
            "sender": {"id": 5}
        });

        let messages = ch.parse_webhook_payload(&payload);
        assert!(messages.is_empty());
    }

    #[test]
    fn skip_missing_conversation_id() {
        let ch = make_channel();
        let payload = serde_json::json!({
            "event": "message_created",
            "account": {"id": 1},
            "message": {
                "content": "Hello",
                "message_type": 0
            },
            "sender": {"id": 5}
        });

        let messages = ch.parse_webhook_payload(&payload);
        assert!(messages.is_empty());
    }

    #[test]
    fn skip_missing_account_id() {
        let ch = make_channel();
        let payload = serde_json::json!({
            "event": "message_created",
            "conversation": {"id": 42},
            "message": {
                "content": "Hello",
                "message_type": 0
            },
            "sender": {"id": 5}
        });

        let messages = ch.parse_webhook_payload(&payload);
        assert!(messages.is_empty());
    }

    #[test]
    fn base_url_trailing_slash_stripped() {
        let ch = ChatwootChannel::new(
            "https://chatwoot.example.com/".to_string(),
            "token".to_string(),
        );
        assert_eq!(ch.base_url, "https://chatwoot.example.com");
    }
}
