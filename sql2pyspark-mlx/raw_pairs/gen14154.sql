WITH src AS (
    SELECT *, CASE WHEN duration_sec >= 491 THEN 'High' WHEN duration_sec >= 116 THEN 'Medium' ELSE 'Low' END AS duration_sec_level
    FROM web_events
    WHERE (event_type <> 'click' AND event_type IN ('search', 'click', 'add_to_cart')) OR event_time >= '2025-05-31'
),
page_summary AS (
    SELECT page, SUM(CASE WHEN event_type = 'add_to_cart' THEN duration_sec ELSE 0 END) AS add_to_cart_duration_sec
    FROM src
    GROUP BY page
)
SELECT page, add_to_cart_duration_sec
FROM page_summary
