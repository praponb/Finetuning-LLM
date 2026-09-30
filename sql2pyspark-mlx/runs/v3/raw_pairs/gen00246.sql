WITH session_summary AS (
    SELECT session_id, ROUND(AVG(duration_sec), 2) AS avg_duration_sec, SUM(duration_sec) AS total_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.id,
    s.browser,
    s.device,
    ss.avg_duration_sec,
    ss.total_duration_sec
FROM sessions s
INNER JOIN session_summary ss ON s.id = ss.session_id
ORDER BY ss.avg_duration_sec, browser
