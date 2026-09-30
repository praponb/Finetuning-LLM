SELECT id, duration_sec, UPPER(event_type) AS event_type_upper
FROM web_events
WHERE duration_sec >= 421
