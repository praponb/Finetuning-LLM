WITH session_summary AS (
    SELECT session_id, MIN(duration_sec) min_duration_sec, AVG(duration_sec) avg_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.device,
    s.country,
    ss.min_duration_sec,
    ss.avg_duration_sec
FROM sessions s
JOIN session_summary ss ON s.id = ss.session_id
WHERE ss.min_duration_sec >= 360
ORDER BY ss.min_duration_sec DESC, s.device DESC;
