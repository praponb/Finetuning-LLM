SELECT TO_DATE(event_time) AS event_day, SUM(CASE WHEN page = '/home' THEN 1 ELSE 0 END) AS home_count, MAX(event_time) AS last_event_time
FROM web_events
GROUP BY event_day
HAVING COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) >= 1
ORDER BY event_day, last_event_time
