WITH src AS (
    SELECT *
    FROM web_events
    WHERE session_id IN (8, 32) AND (page <> '/product' OR HOUR(event_time) BETWEEN 9 AND 15)
),
web_events_stats AS (
    SELECT QUARTER(event_time) AS event_quarter, SUM(CASE WHEN event_type = 'add_to_cart' THEN duration_sec ELSE 0 END) AS add_to_cart_duration_sec, MIN(duration_sec) AS min_duration_sec
    FROM src
    GROUP BY QUARTER(event_time)
)
SELECT event_quarter, add_to_cart_duration_sec
FROM web_events_stats
WHERE add_to_cart_duration_sec > 63
