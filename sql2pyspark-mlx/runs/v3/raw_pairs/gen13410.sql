WITH session_stats AS (
    SELECT session_id, COUNT(duration_sec) duration_sec_count
    FROM web_events
    WHERE session_id IN (17, 38)
    GROUP BY session_id
)
SELECT s.customer_id, s.started_at, ss.duration_sec_count
FROM sessions s
JOIN session_stats ss ON s.id = ss.session_id
ORDER BY duration_sec_count DESC;
