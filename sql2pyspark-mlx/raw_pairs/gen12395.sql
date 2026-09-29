SELECT sessions.browser, sessions.started_at, t.last_event_time
FROM sessions
LEFT JOIN (SELECT session_id, MAX(event_time) AS last_event_time
    FROM web_events
    GROUP BY session_id) t ON sessions.id = t.session_id;
