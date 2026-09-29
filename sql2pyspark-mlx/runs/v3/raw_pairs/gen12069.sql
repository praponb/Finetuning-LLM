WITH session_summary AS (
    SELECT session_id, MAX(event_time) AS last_event_time, MAX(duration_sec) AS max_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.id,
    s.device,
    ss.last_event_time,
    ss.max_duration_sec
FROM sessions s
INNER JOIN session_summary ss ON s.id = ss.session_id
WHERE ss.max_duration_sec >= 390
ORDER BY ss.last_event_time
