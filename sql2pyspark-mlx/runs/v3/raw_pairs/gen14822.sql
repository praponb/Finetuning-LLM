SELECT
    id,
    page,
    duration_sec,
    event_time,
    FIRST_VALUE(event_time) OVER (PARTITION BY page ORDER BY duration_sec DESC, id) AS first_event_time
FROM web_events
