WITH session_stats AS (
    SELECT session_id, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec
    FROM web_events
    WHERE duration_sec <> 148
    GROUP BY session_id
)
SELECT
    s.id,
    s.device,
    s.browser,
    ss.stddev_duration_sec
FROM sessions s
INNER JOIN session_stats ss ON s.id = ss.session_id
WHERE ss.stddev_duration_sec >= 33
ORDER BY ss.stddev_duration_sec DESC
