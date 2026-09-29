SELECT s.id, s.customer_id
FROM sessions s
WHERE s.customer_id = 30 AND s.id IN (SELECT session_id FROM web_events WHERE session_id = 21)
