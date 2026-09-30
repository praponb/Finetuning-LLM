SELECT
    s.customer_id,
    s.device,
    we.event_type,
    we.duration_sec,
    we.event_time
FROM sessions s
JOIN web_events we ON we.session_id = s.id
WHERE we.page IS NOT NULL
ORDER BY event_type
