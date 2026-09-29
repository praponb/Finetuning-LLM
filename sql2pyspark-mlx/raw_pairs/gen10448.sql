SELECT s.id, s.browser
FROM sessions s
WHERE s.customer_id IN (2, 17, 30, 34) AND NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id)
