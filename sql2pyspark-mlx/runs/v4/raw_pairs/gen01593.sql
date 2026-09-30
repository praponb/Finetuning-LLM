SELECT s.id, s.customer_id, s.started_at, s.device FROM sessions s WHERE EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id)
