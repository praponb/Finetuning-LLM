WITH base AS (
    SELECT *
    FROM web_events
    WHERE event_type IN ('purchase', 'add_to_cart')
),
summary AS (
    SELECT page, CASE WHEN YEAR(event_time) < 2024 THEN 'before_2024' WHEN YEAR(event_time) = 2024 THEN '2024' ELSE 'after_2024' END AS event_cohort, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec
    FROM base
    GROUP BY page, CASE WHEN YEAR(event_time) < 2024 THEN 'before_2024' WHEN YEAR(event_time) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT page, event_cohort, stddev_duration_sec
FROM summary
WHERE stddev_duration_sec < 34
ORDER BY event_cohort DESC
