SELECT s.id, s.device
FROM sessions s
WHERE s.id IN (SELECT session_id FROM web_events);
