SELECT event_type, session_id, event_time
FROM web_events
WHERE NOT (event_time >= '2024-05-01' AND event_time >= '2024-06-26')
ORDER BY event_type DESC
