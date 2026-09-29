SELECT
    event_type,
    SUM(CASE WHEN page = '/cart' THEN 1 ELSE 0 END) AS cart_count,
    AVG(duration_sec) AS avg_duration_sec,
    SUM(CASE WHEN page = '/cart' THEN duration_sec ELSE 0 END) AS cart_duration_sec
FROM web_events
GROUP BY event_type
HAVING COUNT(CASE WHEN page = '/home' THEN 1 END) < 5
ORDER BY avg_duration_sec DESC
