WITH session_summary AS (
    SELECT session_id, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT s.id, s.customer_id, ss.stddev_duration_sec
FROM sessions s
JOIN session_summary ss ON s.id = ss.session_id;
