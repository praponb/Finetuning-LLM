SELECT
    sessions.id,
    sessions.device,
    t.stddev_duration_sec,
    t.latest_event_time
FROM sessions
JOIN (SELECT session_id, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec, MAX(event_time) AS latest_event_time
    FROM web_events
    GROUP BY session_id) t ON sessions.id = t.session_id
WHERE t.stddev_duration_sec >= 59
