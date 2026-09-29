SELECT
    s.id,
    s.country,
    s.device,
    t.min_event_time
FROM sessions s
JOIN (SELECT session_id, MIN(event_time) AS min_event_time
    FROM web_events
    WHERE duration_sec BETWEEN 135 AND 346
    GROUP BY session_id) t ON s.id = t.session_id;
