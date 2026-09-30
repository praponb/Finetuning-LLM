SELECT
    s.id,
    s.country,
    s.browser,
    t.latest_event_time
FROM sessions s
LEFT JOIN (SELECT session_id, MAX(event_time) AS latest_event_time
    FROM web_events
    GROUP BY session_id) t ON s.id = t.session_id
ORDER BY id DESC
