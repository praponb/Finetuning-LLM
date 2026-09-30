SELECT event_type, duration_sec
FROM web_events
WHERE duration_sec IS NOT NULL
