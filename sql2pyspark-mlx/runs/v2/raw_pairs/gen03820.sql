WITH session_stats AS (
    SELECT session_id, MAX(duration_sec) AS max_duration_sec
    FROM web_events
    WHERE TO_DATE(event_time) >= '2024-06-01'
    GROUP BY session_id
)
SELECT
    s.id,
    s.browser,
    s.started_at,
    ss.max_duration_sec
FROM sessions s
INNER JOIN session_stats ss ON s.id = ss.session_id;
