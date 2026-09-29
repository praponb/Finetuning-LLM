SELECT duration_sec, session_id, event_type
FROM web_events
WHERE event_type = 'search'
