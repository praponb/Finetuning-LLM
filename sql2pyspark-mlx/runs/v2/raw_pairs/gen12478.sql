WITH web_events_by_session AS (
    SELECT session_id, COUNT(*) AS web_event_count
    FROM web_events
    GROUP BY session_id
)
SELECT sessions.id, sessions.device, web_events_by_session.web_event_count
FROM sessions
JOIN web_events_by_session ON sessions.id = web_events_by_session.session_id
ORDER BY sessions.id DESC, web_events_by_session.web_event_count DESC
LIMIT 50;
