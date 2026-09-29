WITH session_stats AS (
    SELECT session_id, ROUND(AVG(duration_sec), 2) AS avg_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.id,
    s.browser,
    s.started_at,
    ss.avg_duration_sec
FROM sessions s
INNER JOIN session_stats ss ON s.id = ss.session_id;
