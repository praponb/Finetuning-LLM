SELECT session_id, TRIM(event_type) AS clean_event_type
FROM web_events
ORDER BY clean_event_type;
