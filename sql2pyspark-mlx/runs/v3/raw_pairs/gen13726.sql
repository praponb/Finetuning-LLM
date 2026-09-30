SELECT
    sessions.id,
    sessions.started_at,
    sessions.device,
    sessions.country
FROM sessions
WHERE sessions.id IN (SELECT session_id FROM web_events WHERE duration_sec BETWEEN 285 AND 462);
