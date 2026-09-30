SELECT
    s.id,
    s.browser,
    COUNT(we.id) AS web_event_count,
    SUM(we.duration_sec) AS total_duration_sec
FROM sessions s
INNER JOIN web_events we ON we.session_id = s.id
GROUP BY s.id, s.browser
HAVING web_event_count >= 2
ORDER BY web_event_count DESC
