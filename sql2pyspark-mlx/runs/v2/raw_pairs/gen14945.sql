SELECT
    id,
    duration_sec,
    event_type,
    event_time
FROM web_events
WHERE NOT (event_type = 'page_view' AND (event_type = 'add_to_cart' OR duration_sec < 233))
