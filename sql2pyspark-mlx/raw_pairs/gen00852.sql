WITH session_stats AS (
    SELECT session_id, COUNT(duration_sec) AS duration_sec_count
    FROM web_events
    GROUP BY session_id
)
SELECT s.id, s.started_at, ss.duration_sec_count
FROM sessions s
JOIN session_stats ss ON s.id = ss.session_id
ORDER BY started_at DESC, id
LIMIT 50;
