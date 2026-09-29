SELECT
    id,
    duration_sec,
    session_id,
    page
FROM web_events
WHERE session_id IN (18, 39) AND duration_sec >= 279;
