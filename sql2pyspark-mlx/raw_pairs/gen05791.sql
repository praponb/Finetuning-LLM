WITH session_summary AS (
    SELECT session_id, COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) add_to_cart_count
    FROM web_events
    GROUP BY session_id
)
SELECT sessions.id, sessions.started_at, session_summary.add_to_cart_count
FROM sessions
INNER JOIN session_summary ON sessions.id = session_summary.session_id
WHERE session_summary.add_to_cart_count >= 4
ORDER BY session_summary.add_to_cart_count DESC;
