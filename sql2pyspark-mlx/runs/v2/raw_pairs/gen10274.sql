SELECT
    page,
    event_type,
    MIN(duration_sec) AS min_duration_sec,
    COUNT(CASE WHEN page = '/product' THEN 1 END) AS product_count,
    COUNT(duration_sec) AS duration_sec_count
FROM web_events
WHERE HOUR(event_time) BETWEEN 9 AND 15
GROUP BY CUBE(page, event_type);
