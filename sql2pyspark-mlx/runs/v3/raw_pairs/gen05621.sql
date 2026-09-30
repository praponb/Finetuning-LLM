WITH session_stats AS (
    SELECT session_id, MAX(duration_sec) AS max_duration_sec
    FROM web_events
    WHERE page IN ('/product', '/home', '/search')
    GROUP BY session_id
)
SELECT s.id, s.customer_id, ss.max_duration_sec
FROM sessions s
INNER JOIN session_stats ss ON s.id = ss.session_id;
