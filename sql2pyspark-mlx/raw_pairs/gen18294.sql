SELECT event_type, SUM(CASE WHEN page = '/cart' THEN 1 ELSE 0 END) AS cart_count, MIN(duration_sec) AS min_duration_sec
FROM web_events
GROUP BY event_type
ORDER BY cart_count DESC
