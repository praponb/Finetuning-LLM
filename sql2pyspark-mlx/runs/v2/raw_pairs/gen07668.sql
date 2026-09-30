SELECT id, event_time
FROM web_events
WHERE duration_sec IS NOT NULL;
