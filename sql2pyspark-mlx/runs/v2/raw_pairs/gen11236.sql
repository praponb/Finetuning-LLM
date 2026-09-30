SELECT
    s.country,
    COUNT(we.id) AS web_event_count,
    MAX(we.duration_sec) - MIN(we.duration_sec) AS duration_sec_range,
    MAX(we.event_time) AS last_event_time
FROM sessions s
JOIN web_events we ON we.session_id = s.id
GROUP BY s.country
ORDER BY country
LIMIT 5;
