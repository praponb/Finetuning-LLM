SELECT *
FROM (
    SELECT
        page,
        event_type,
        AVG(duration_sec) AS avg_duration_sec,
        SUM(CASE WHEN page = '/product' THEN 1 ELSE 0 END) AS product_count
    FROM web_events
    GROUP BY page, event_type
) AS t
WHERE avg_duration_sec > 340
ORDER BY avg_duration_sec ASC
