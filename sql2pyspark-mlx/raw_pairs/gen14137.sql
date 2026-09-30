WITH session_stats AS (
    SELECT session_id, AVG(duration_sec) AS avg_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.id,
    s.device,
    s.customer_id,
    ss.avg_duration_sec
FROM sessions s
LEFT JOIN session_stats ss ON s.id = ss.session_id
ORDER BY customer_id DESC;
