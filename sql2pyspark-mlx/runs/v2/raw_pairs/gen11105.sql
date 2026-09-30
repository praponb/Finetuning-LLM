SELECT
    event_type,
    SUM(CASE WHEN page = '/home' THEN duration_sec ELSE 0 END) AS home_duration_sec,
    MAX(duration_sec) AS max_duration_sec,
    MIN(duration_sec) AS min_duration_sec
FROM web_events
WHERE session_id = 2
GROUP BY 1
HAVING SUM(CASE WHEN page = '/home' THEN duration_sec ELSE 0 END) >= 32
ORDER BY event_type;
