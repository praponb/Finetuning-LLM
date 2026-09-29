SELECT event_time, SUBSTRING(event_type, 1, 4) AS event_type_prefix
FROM web_events
WHERE duration_sec = 211 AND event_type = 'search'
