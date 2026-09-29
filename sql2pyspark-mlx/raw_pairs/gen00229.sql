SELECT
    id,
    event_type,
    page,
    session_id,
    TRIM(event_type) AS clean_event_type,
    LOWER(page) AS page_lower
FROM web_events
WHERE session_id IN (5, 27)
