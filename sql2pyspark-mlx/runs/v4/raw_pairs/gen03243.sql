SELECT
    id,
    page,
    duration_sec,
    CONCAT_WS('/', page, event_type) page_event_type_key
FROM web_events
WHERE event_type IN ('page_view', 'click', 'search')
ORDER BY page_event_type_key, id DESC
