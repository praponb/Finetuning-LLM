SELECT
    id,
    page,
    event_time,
    duration_sec
FROM web_events
ORDER BY event_time DESC, page ASC
