SELECT
    id,
    event_time,
    event_type,
    session_id
FROM web_events
WHERE session_id = 21 AND session_id IN (2, 6, 10, 12) AND session_id IN (1, 12, 26, 29);
