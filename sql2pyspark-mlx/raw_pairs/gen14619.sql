SELECT
    sessions.id,
    sessions.device,
    sessions.country,
    sessions.started_at
FROM sessions
WHERE sessions.country <> 'UK' AND NOT EXISTS (SELECT 1 FROM web_events WHERE web_events.session_id = sessions.id AND web_events.duration_sec <= 470)
