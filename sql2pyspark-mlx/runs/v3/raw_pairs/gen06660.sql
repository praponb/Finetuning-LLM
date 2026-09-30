SELECT
    id,
    event_time,
    session_id,
    CONCAT_WS('/', event_type, page) AS event_type_page_key
FROM web_events
WHERE session_id = 15 AND duration_sec > 366
