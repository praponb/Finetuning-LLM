SELECT
    id,
    duration_sec,
    event_time,
    page
FROM web_events
WHERE duration_sec > (SELECT MAX(duration_sec) FROM web_events)
ORDER BY duration_sec;
