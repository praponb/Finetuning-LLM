WITH session_stats AS (
    SELECT session_id, MIN(event_time) first_event_time
    FROM web_events
    GROUP BY session_id
)
SELECT s.device, s.started_at, ss.first_event_time
FROM sessions s
JOIN session_stats ss ON s.id = ss.session_id
ORDER BY 3 DESC;
