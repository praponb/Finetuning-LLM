SELECT s.id, s.customer_id, s.device FROM sessions s WHERE s.started_at < '2025-07-01' AND NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id) ORDER BY device DESC
