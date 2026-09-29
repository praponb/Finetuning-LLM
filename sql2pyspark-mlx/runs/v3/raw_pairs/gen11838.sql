WITH valid_web_events AS (
    SELECT *
    FROM web_events
    WHERE event_type IN ('purchase', 'search') AND event_type IN ('purchase', 'add_to_cart')
),
agg AS (
    SELECT page, event_type, SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_count
    FROM valid_web_events
    GROUP BY page, event_type
)
SELECT page, add_to_cart_count
FROM agg
WHERE add_to_cart_count > 35
ORDER BY page DESC, add_to_cart_count DESC;
