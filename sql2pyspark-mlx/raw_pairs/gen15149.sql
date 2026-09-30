SELECT
    s.id,
    s.device,
    COUNT(we.id) web_event_count,
    ROUND(AVG(we.duration_sec), 2) avg_duration_sec
FROM sessions s
LEFT JOIN web_events we ON we.session_id = s.id
WHERE s.browser IS NULL
GROUP BY s.id, s.device
