WITH session_summary AS (
    SELECT session_id, MAX(duration_sec) AS max_duration_sec, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT s.browser, ss.max_duration_sec, ss.stddev_duration_sec
FROM sessions s
JOIN session_summary ss ON s.id = ss.session_id;
