SELECT
    s.customer_id,
    s.started_at,
    we.duration_sec,
    we.session_id,
    we.page
FROM sessions s
LEFT JOIN web_events we ON we.session_id = s.id
ORDER BY page DESC, duration_sec;
