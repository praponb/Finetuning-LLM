SELECT
    s.device,
    s.started_at,
    s.browser,
    we.event_time,
    we.event_type,
    we.session_id
FROM sessions s
JOIN web_events we ON s.id = we.session_id
ORDER BY started_at;
