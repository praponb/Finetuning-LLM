SELECT event_type, COUNT(*) AS cnt
FROM web_events
WHERE TO_DATE(event_time) >= '2024-09-01'
GROUP BY event_type
HAVING cnt >= 3
ORDER BY event_type
