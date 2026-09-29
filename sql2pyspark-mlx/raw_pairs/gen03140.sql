SELECT
    id,
    event_type,
    event_time,
    duration_sec,
    LAG(duration_sec, 1) OVER (PARTITION BY event_type ORDER BY event_time, id) prev_duration_sec
FROM web_events
