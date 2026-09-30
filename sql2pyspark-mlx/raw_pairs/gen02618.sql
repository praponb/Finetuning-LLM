SELECT sessions.id, sessions.started_at, sessions.country
FROM sessions
WHERE sessions.id IN (SELECT session_id FROM web_events WHERE session_id IN (26, 29, 30))
ORDER BY country, id
LIMIT 5;
