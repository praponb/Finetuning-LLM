WITH session_summary AS (
    SELECT session_id, ROUND(AVG(duration_sec), 2) avg_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT s.browser, ss.avg_duration_sec
FROM sessions s
LEFT JOIN session_summary ss ON s.id = ss.session_id;
