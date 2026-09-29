SELECT *, LOWER(event_type) event_type_lower
FROM web_events
WHERE duration_sec IS NOT NULL;
