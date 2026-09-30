WITH session_summary AS (
    SELECT session_id, AVG(duration_sec) AS avg_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT
    sessions.id,
    sessions.device,
    sessions.started_at,
    session_summary.avg_duration_sec
FROM sessions
INNER JOIN session_summary ON sessions.id = session_summary.session_id
ORDER BY avg_duration_sec;
