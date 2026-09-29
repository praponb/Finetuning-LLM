SELECT
    id,
    event_type,
    event_time,
    duration_sec,
    SUM(duration_sec) OVER (PARTITION BY event_type ORDER BY event_time, id) AS running_duration_sec
FROM web_events;
