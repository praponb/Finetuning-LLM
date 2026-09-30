SELECT
    id,
    duration_sec,
    page,
    session_id
FROM web_events
WHERE duration_sec >= (SELECT AVG(duration_sec) FROM web_events);
