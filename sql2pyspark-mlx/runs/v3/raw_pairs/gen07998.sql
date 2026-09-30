SELECT
    id,
    session_id,
    event_time,
    page,
    TRIM(event_type) AS clean_event_type,
    SUBSTRING(page, 1, 2) AS page_prefix
FROM web_events
ORDER BY page;
