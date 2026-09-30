WITH session_summary AS (
    SELECT session_id, MIN(duration_sec) AS min_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT s.browser, ss.min_duration_sec
FROM sessions s
INNER JOIN session_summary ss ON s.id = ss.session_id
