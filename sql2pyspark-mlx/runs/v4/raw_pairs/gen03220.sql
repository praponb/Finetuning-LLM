WITH session_stats AS (
    SELECT session_id, SUM(CASE WHEN page = '/product' THEN duration_sec ELSE 0 END) AS product_duration_sec
    FROM web_events
    WHERE event_time < '2025-04-01'
    GROUP BY session_id
)
SELECT s.id, s.started_at, ss.product_duration_sec
FROM sessions s
INNER JOIN session_stats ss ON s.id = ss.session_id
WHERE ss.product_duration_sec > 58
ORDER BY ss.product_duration_sec DESC
