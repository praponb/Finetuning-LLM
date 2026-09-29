SELECT
    s.country,
    s.started_at,
    we.event_time,
    we.duration_sec,
    we.event_type
FROM sessions s
LEFT JOIN web_events we ON s.id = we.session_id
