SELECT s.id, s.customer_id
FROM sessions s
WHERE s.started_at < '2025-07-01' AND NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.session_id IN (8, 38))
ORDER BY id DESC;
