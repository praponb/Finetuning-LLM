SELECT
    sessions.id,
    sessions.device,
    COUNT(web_events.id) AS web_event_count,
    MIN(web_events.event_time) AS first_event_time
FROM sessions
LEFT JOIN web_events ON sessions.id = web_events.session_id
WHERE web_events.event_time < '2024-12-01'
GROUP BY sessions.id, sessions.device
