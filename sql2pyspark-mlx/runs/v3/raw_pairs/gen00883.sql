SELECT
    id,
    session_id,
    event_type,
    event_time
FROM web_events
WHERE event_type = 'add_to_cart' AND session_id = 11
ORDER BY event_type DESC, event_time, id
LIMIT 5
