SELECT event_time
FROM web_events
WHERE session_id IN (9, 30, 40) AND event_time >= '2025-09-01'
