SELECT
    HOUR(event_time) AS event_hour,
    SUM(CASE WHEN event_type = 'search' THEN duration_sec ELSE 0 END) AS search_duration_sec,
    COUNT(*) AS row_count,
    MIN(duration_sec) AS min_duration_sec
FROM web_events
WHERE session_id = 29 AND page IS NULL
GROUP BY HOUR(event_time)
ORDER BY min_duration_sec DESC;
