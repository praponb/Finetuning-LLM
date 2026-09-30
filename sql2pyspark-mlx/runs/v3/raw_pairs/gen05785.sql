WITH web_events_by_session AS (
    SELECT session_id, MAX(event_time) AS latest_event_time
    FROM web_events
    GROUP BY session_id
)
SELECT
    sessions.id,
    sessions.country,
    sessions.device,
    web_events_by_session.latest_event_time
FROM sessions
LEFT JOIN web_events_by_session ON sessions.id = web_events_by_session.session_id
ORDER BY sessions.id DESC;
