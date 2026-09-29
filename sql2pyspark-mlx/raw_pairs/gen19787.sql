SELECT
    we.id,
    we.event_time,
    we.event_type,
    s.device,
    s.started_at
FROM web_events we
LEFT JOIN sessions s ON s.id = we.session_id
ORDER BY we.event_time
