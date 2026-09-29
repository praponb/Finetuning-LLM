WITH filtered_web_events AS (
    SELECT *
    FROM web_events
    WHERE duration_sec != 463
),
Summary AS (
    SELECT page, AVG(duration_sec) AS avg_duration_sec, SUM(CASE WHEN page = '/product' THEN 1 ELSE 0 END) AS product_count
    FROM filtered_web_events
    GROUP BY page
)
SELECT page, avg_duration_sec, product_count
FROM Summary
WHERE avg_duration_sec > 160
ORDER BY product_count ASC, avg_duration_sec DESC
