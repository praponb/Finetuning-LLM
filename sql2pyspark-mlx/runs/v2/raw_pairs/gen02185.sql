SELECT id, event_type
FROM web_events
WHERE session_id IN (13, 21, 35, 38)
ORDER BY event_type ASC, id DESC;
