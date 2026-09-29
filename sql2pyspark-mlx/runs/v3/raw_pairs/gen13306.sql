SELECT HOUR(event_time) AS event_hour, COUNT(CASE WHEN event_type = 'search' THEN 1 END) AS search_count
FROM web_events
WHERE page != '/cart' AND duration_sec > 331
GROUP BY HOUR(event_time)
HAVING COUNT(CASE WHEN event_type = 'search' THEN 1 END) >= 5
