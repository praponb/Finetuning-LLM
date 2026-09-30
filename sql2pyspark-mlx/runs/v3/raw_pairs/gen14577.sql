SELECT
    id,
    duration_sec,
    event_time,
    INITCAP(TRIM(event_type)) AS event_type_title
FROM web_events
WHERE session_id IN (4, 8, 15) AND page = '/cart'
