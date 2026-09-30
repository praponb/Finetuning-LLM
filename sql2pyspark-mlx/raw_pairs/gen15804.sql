SELECT
    page,
    MIN(event_time) AS min_event_time,
    SUM(duration_sec) AS total_duration_sec,
    COUNT(*) AS num_web_events
FROM web_events
GROUP BY page
HAVING COUNT(*) > 3
ORDER BY 2, page
LIMIT 50;
