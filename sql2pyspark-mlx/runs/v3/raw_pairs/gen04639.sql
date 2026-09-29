WITH base AS (
    SELECT *
    FROM web_events
    WHERE event_time >= '2023-04-15' AND event_time < '2024-02-01'
),
event_type_summary AS (
    SELECT event_type, SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count, MIN(event_time) AS first_event_time
    FROM base
    GROUP BY event_type
)
SELECT event_type, first_event_time
FROM event_type_summary
WHERE purchase_count < 49;
