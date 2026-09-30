WITH src AS (
    SELECT *, CAST(event_time AS DATE) AS event_time_day
    FROM web_events
    WHERE page = '/product' AND event_time >= '2024-03-30'
),
summary AS (
    SELECT MONTH(event_time_day) AS event_day_month, SUM(CASE WHEN page = '/home' THEN duration_sec ELSE 0 END) AS home_duration_sec, MIN(event_time_day) AS first_event_time_day
    FROM src
    GROUP BY MONTH(event_time_day)
)
SELECT event_day_month, first_event_time_day
FROM summary
WHERE home_duration_sec > 45
ORDER BY first_event_time_day
