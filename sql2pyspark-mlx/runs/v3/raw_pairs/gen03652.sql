SELECT s.id, s.started_at, s.customer_id
FROM sessions s
WHERE NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.page <> '/cart')
ORDER BY customer_id, id
LIMIT 50
