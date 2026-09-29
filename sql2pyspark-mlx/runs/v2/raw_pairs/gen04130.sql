SELECT s.id, s.customer_id, s.browser
FROM sessions s
WHERE NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.duration_sec > 198)
ORDER BY 3 DESC, customer_id DESC
