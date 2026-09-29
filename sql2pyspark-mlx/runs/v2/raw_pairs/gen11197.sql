SELECT page, event_type, duration_sec
FROM web_events
ORDER BY duration_sec DESC, page
