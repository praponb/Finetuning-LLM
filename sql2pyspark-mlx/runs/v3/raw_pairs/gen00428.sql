SELECT session_id, event_time, duration_sec
FROM web_events
WHERE event_type != 'click' AND TO_DATE(event_time) < '2024-08-01'
