SELECT
    id,
    page,
    event_time,
    duration_sec,
    SUM(duration_sec) OVER (PARTITION BY page ORDER BY event_time, id) running_duration_sec
FROM web_events
ORDER BY running_duration_sec DESC
