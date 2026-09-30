SELECT s.id, s.customer_id, s.country
FROM sessions s
WHERE s.browser = 'Safari' AND s.id IN (SELECT session_id FROM web_events WHERE session_id = 1);
