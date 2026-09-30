SELECT *, UPPER(event_type) AS event_type_upper, CONCAT(page, ' - ', event_type) AS page_event_type
FROM web_events
WHERE duration_sec = 196
