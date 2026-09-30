WITH recent_web_events AS (
    SELECT *
    FROM web_events
    WHERE event_type = 'add_to_cart' AND duration_sec < 292 AND session_id = 10
),
web_events_stats AS (
    SELECT page, MONTH(event_time) AS event_month, SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count
    FROM recent_web_events
    GROUP BY page, MONTH(event_time)
)
SELECT page, event_month, purchase_count
FROM web_events_stats
ORDER BY page
