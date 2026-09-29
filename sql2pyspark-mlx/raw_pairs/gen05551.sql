SELECT
    s.id,
    s.country,
    s.browser,
    t.first_event_time
FROM sessions s
INNER JOIN (SELECT session_id, MIN(event_time) AS first_event_time
    FROM web_events
    GROUP BY session_id) t ON s.id = t.session_id
