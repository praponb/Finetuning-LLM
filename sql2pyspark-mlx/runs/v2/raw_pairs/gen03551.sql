WITH web_events_by_session AS (
    SELECT session_id, MAX(duration_sec) AS max_duration_sec, MIN(event_time) AS min_event_time
    FROM web_events
    GROUP BY session_id
)
SELECT sessions.country, web_events_by_session.max_duration_sec, web_events_by_session.min_event_time
FROM sessions
JOIN web_events_by_session ON sessions.id = web_events_by_session.session_id
WHERE web_events_by_session.max_duration_sec >= 140
