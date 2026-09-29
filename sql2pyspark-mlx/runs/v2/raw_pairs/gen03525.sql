SELECT s.id, s.country
FROM sessions s
WHERE s.id IN (SELECT session_id FROM web_events)
ORDER BY id;
