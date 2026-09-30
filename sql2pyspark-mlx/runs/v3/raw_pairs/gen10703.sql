SELECT
    id,
    session_id,
    event_type,
    COALESCE(duration_sec, 0) AS duration_sec_filled
FROM web_events
WHERE session_id IN (20, 24, 27, 39)
