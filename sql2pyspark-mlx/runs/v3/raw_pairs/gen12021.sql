SELECT
    s.started_at,
    s.device,
    we.event_type,
    we.duration_sec
FROM sessions s
LEFT JOIN web_events we ON s.id = we.session_id
ORDER BY s.device, duration_sec DESC;
