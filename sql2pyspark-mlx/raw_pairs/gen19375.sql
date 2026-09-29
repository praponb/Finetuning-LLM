WITH session_stats AS (
    SELECT session_id, COUNT(duration_sec) AS duration_sec_count, MAX(event_time) AS latest_event_time
    FROM web_events
    GROUP BY session_id
)
SELECT sessions.started_at, session_stats.duration_sec_count, session_stats.latest_event_time
FROM sessions
INNER JOIN session_stats ON sessions.id = session_stats.session_id
WHERE session_stats.duration_sec_count > 9
ORDER BY 1, session_stats.duration_sec_count DESC;
