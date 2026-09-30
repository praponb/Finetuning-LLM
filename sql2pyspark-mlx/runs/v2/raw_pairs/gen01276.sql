WITH base AS (
    SELECT *
    FROM web_events
    WHERE event_time < '2023-11-07'
),
event_type_summary AS (
    SELECT event_type, ROUND((100 * SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_add_to_cart
    FROM base
    GROUP BY event_type
)
SELECT event_type, pct_add_to_cart
FROM event_type_summary;
