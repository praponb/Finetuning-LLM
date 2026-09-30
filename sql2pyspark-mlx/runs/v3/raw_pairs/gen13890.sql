SELECT
    event_type,
    page,
    SUM(CASE WHEN page = '/product' THEN 1 ELSE 0 END) product_count,
    ROUND(AVG(duration_sec), 2) avg_duration_sec
FROM web_events
WHERE duration_sec <= 382 AND HOUR(event_time) BETWEEN 11 AND 15
GROUP BY event_type, page
HAVING avg_duration_sec >= 46
