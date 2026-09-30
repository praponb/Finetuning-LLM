WITH SessionStats AS (
    SELECT session_id, SUM(CASE WHEN page = '/cart' THEN 1 ELSE 0 END) AS cart_count, SUM(CASE WHEN page = '/search' THEN duration_sec ELSE 0 END) AS search_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT
    sessions.browser,
    sessions.customer_id,
    SessionStats.cart_count,
    SessionStats.search_duration_sec
FROM sessions
INNER JOIN SessionStats ON sessions.id = SessionStats.session_id
WHERE SessionStats.search_duration_sec >= 69
