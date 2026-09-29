SELECT
    s.browser,
    s.device,
    s.started_at,
    we.duration_sec,
    we.page
FROM sessions s
LEFT JOIN web_events we ON s.id = we.session_id
ORDER BY duration_sec DESC
