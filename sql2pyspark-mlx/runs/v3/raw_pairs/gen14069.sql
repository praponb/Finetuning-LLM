SELECT
    sessions.country,
    web_events.duration_sec,
    web_events.event_type,
    web_events.session_id
FROM sessions
LEFT OUTER JOIN web_events ON sessions.id = web_events.session_id
