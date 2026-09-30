SELECT
    s.id,
    s.device,
    COUNT(we.id) AS web_event_count,
    MIN(we.event_time) AS first_event_time,
    COUNT(DISTINCT we.duration_sec) AS unique_event_types
FROM sessions s
LEFT JOIN web_events we ON we.session_id = s.id
GROUP BY s.id, s.device
ORDER BY 2
