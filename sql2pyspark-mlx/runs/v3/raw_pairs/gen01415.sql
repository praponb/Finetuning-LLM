WITH filtered_web_events AS (
    SELECT *
    FROM web_events
    WHERE event_time >= '2024-06-01' AND duration_sec > 233
),
event_month_summary AS (
    SELECT
        DATE_TRUNC('MONTH', event_time) AS event_month,
        CASE WHEN duration_sec >= 402 THEN 'high' WHEN duration_sec >= 225 THEN 'mid' ELSE 'low' END AS duration_sec_bucket,
        ROUND((100 * SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_add_to_cart,
        SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list
    FROM filtered_web_events
    GROUP BY DATE_TRUNC('MONTH', event_time), CASE WHEN duration_sec >= 402 THEN 'high' WHEN duration_sec >= 225 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM event_month_summary
ORDER BY pct_add_to_cart DESC, duration_sec_bucket, event_month
LIMIT 20;
