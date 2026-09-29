SELECT DISTINCT page, event_type
FROM web_events
WHERE TO_DATE(event_time) >= '2025-09-14'
