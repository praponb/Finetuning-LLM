SELECT we.id, we.session_id, s.started_at, s.customer_id FROM web_events we JOIN sessions s ON s.id = we.session_id
