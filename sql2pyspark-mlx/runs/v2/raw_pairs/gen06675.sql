SELECT id, event_time, INITCAP(TRIM(page)) page_title
FROM web_events
WHERE event_type = 'page_view' AND event_type = 'click'
