SELECT
    id,
    event_time,
    duration_sec,
    page,
    INITCAP(TRIM(event_type)) AS event_type_title,
    LOWER(page) AS page_lower
FROM web_events
WHERE session_id IN (12, 23, 28, 30);
