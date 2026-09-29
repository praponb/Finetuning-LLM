SELECT
    id,
    session_id,
    event_time,
    duration_sec,
    event_type,
    SUM(duration_sec) OVER (PARTITION BY session_id ORDER BY event_time, id) running_duration_sec,
    ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time DESC, id) rn
FROM web_events
