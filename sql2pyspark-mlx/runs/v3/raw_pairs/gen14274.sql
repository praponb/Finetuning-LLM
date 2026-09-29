SELECT event_type, ROUND(AVG(duration_sec), 2) avg_duration_sec, SUM(CASE WHEN event_type = 'search' THEN duration_sec ELSE 0 END) search_duration_sec
FROM web_events
WHERE TO_DATE(event_time) < '2025-06-20'
GROUP BY event_type
ORDER BY avg_duration_sec, event_type
LIMIT 10
