SELECT duration_sec, event_type, TRIM(page) AS clean_page
FROM web_events
