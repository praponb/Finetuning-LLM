WITH session_summary AS (
    SELECT session_id, SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_count, MIN(event_time) AS min_event_time
    FROM web_events
    GROUP BY session_id
)
SELECT
    sessions.started_at,
    sessions.device,
    session_summary.add_to_cart_count,
    session_summary.min_event_time
FROM sessions
INNER JOIN session_summary ON sessions.id = session_summary.session_id
WHERE session_summary.add_to_cart_count >= 39
ORDER BY started_at, min_event_time DESC
