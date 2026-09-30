SELECT
    s.browser,
    s.started_at,
    s.device,
    we.duration_sec,
    we.event_type
FROM sessions s
LEFT JOIN web_events we ON s.id = we.session_id
WHERE we.duration_sec IS NULL AND we.page IS NULL
ORDER BY device ASC
