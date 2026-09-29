SELECT page, event_type, duration_sec
FROM web_events
WHERE HOUR(event_time) BETWEEN 12 AND 20 AND TO_DATE(event_time) < '2025-09-01'
