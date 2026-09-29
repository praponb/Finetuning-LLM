SELECT
    s.started_at,
    s.country,
    we.page,
    we.event_time,
    we.event_type
FROM sessions s
LEFT JOIN web_events we ON s.id = we.session_id
ORDER BY event_time;
