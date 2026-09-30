SELECT
    id,
    session_id,
    event_time,
    CONCAT_WS(',', event_type, page) event_type_page_key,
    DATE_TRUNC('year', event_time) event_time_period
FROM web_events
ORDER BY event_time, id
LIMIT 5;
