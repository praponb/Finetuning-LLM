SELECT
    id,
    event_time,
    CAST(duration_sec AS STRING) AS duration_sec_num,
    CONCAT(event_type, '/', page) AS event_type_page
FROM web_events
WHERE session_id IN (12, 34)
