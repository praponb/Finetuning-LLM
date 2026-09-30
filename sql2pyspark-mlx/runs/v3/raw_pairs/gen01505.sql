SELECT DISTINCT page, event_type
FROM web_events
WHERE TO_DATE(event_time) < '2024-12-01'
ORDER BY event_type DESC;
