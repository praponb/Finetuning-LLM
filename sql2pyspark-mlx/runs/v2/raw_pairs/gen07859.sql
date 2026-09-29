WITH session_summary AS (
    SELECT session_id, SUM(CASE WHEN page = '/home' THEN 1 ELSE 0 END) AS home_count, AVG(duration_sec) AS avg_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.id,
    s.country,
    s.device,
    ss.home_count,
    ss.avg_duration_sec
FROM sessions s
INNER JOIN session_summary ss ON s.id = ss.session_id
ORDER BY s.country DESC;
