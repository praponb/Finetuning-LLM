SELECT
    id,
    session_id,
    event_time,
    event_type
FROM web_events
ORDER BY id
LIMIT 1;
