SELECT
    id,
    duration_sec,
    page,
    event_time
FROM web_events
WHERE duration_sec > (SELECT MAX(duration_sec) FROM web_events WHERE event_time >= '2023-12-15')
ORDER BY duration_sec DESC, id
LIMIT 3;
