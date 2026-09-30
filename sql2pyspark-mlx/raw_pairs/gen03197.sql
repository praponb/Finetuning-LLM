WITH valid_web_events AS (
    SELECT *
    FROM web_events
    WHERE duration_sec = 317 AND (event_type = 'add_to_cart' OR page NOT IN ('/cart', '/home'))
),
agg AS (
    SELECT event_type, SUM(CASE WHEN page = '/cart' THEN duration_sec ELSE 0 END) AS cart_duration_sec
    FROM valid_web_events
    GROUP BY event_type
)
SELECT event_type, cart_duration_sec
FROM agg
ORDER BY cart_duration_sec DESC, event_type
LIMIT 5;
