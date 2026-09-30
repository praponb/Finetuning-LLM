SELECT s.id, s.customer_id FROM sessions s WHERE HOUR(s.started_at) BETWEEN 12 AND 18 AND EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id)
