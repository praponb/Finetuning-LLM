WITH session_summary AS (
    SELECT session_id, SUM(duration_sec) AS total_duration_sec, AVG(duration_sec) AS avg_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT s.country, ss.total_duration_sec, ss.avg_duration_sec
FROM sessions s
LEFT JOIN session_summary ss ON s.id = ss.session_id
ORDER BY total_duration_sec DESC, ss.avg_duration_sec ASC;
