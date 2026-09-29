SELECT
    s.id,
    s.device,
    s.browser,
    t.first_event_time,
    t.last_event_time
FROM sessions s
JOIN (SELECT session_id, MIN(event_time) AS first_event_time, MAX(event_time) AS last_event_time
    FROM web_events
    GROUP BY session_id) t ON s.id = t.session_id
ORDER BY s.browser
