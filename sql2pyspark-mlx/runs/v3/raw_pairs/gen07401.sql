SELECT
    id,
    duration_sec,
    event_type,
    session_id
FROM web_events
WHERE event_type = 'search' OR page IS NOT NULL
