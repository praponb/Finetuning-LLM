WITH session_summary AS (
    SELECT session_id, MAX(duration_sec) AS max_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT s.browser, ss.max_duration_sec
FROM sessions s
LEFT JOIN session_summary ss ON s.id = ss.session_id
WHERE ss.max_duration_sec >= 300
ORDER BY s.browser, max_duration_sec;
