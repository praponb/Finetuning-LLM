SELECT s.id, s.device, s.browser
FROM sessions s
WHERE s.id IN (SELECT session_id FROM web_events WHERE session_id IN (12, 14, 17, 38))
ORDER BY s.id
LIMIT 5;
