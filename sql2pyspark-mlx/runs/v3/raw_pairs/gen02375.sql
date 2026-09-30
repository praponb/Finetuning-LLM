SELECT session_id, event_type, event_time
FROM web_events
WHERE duration_sec IS NOT NULL
