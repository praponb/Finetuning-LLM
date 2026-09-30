SELECT
    s.country,
    s.started_at,
    we.duration_sec,
    we.page
FROM sessions s
INNER JOIN web_events we ON s.id = we.session_id
ORDER BY s.country
