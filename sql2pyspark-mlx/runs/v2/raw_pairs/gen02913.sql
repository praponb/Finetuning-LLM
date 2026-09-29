WITH valid_web_events AS (
    SELECT *, CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END AS is_add_to_cart
    FROM web_events
    WHERE session_id IN (1, 6, 9) AND session_id IN (18, 23, 40)
),
web_events_stats AS (
    SELECT
        TO_DATE(event_time) AS event_day,
        session_id,
        MAX(is_add_to_cart) - MIN(is_add_to_cart) AS is_add_to_cart_range,
        COUNT(is_add_to_cart) AS is_add_to_cart_count
    FROM valid_web_events
    GROUP BY TO_DATE(event_time), session_id
)
SELECT
    event_day,
    session_id,
    is_add_to_cart_range,
    is_add_to_cart_count
FROM web_events_stats
WHERE is_add_to_cart_count < 7
ORDER BY is_add_to_cart_count ASC, event_day ASC, session_id ASC
LIMIT 5;
