SELECT event_type, SUM(CASE WHEN event_type = 'add_to_cart' THEN duration_sec ELSE 0 END) AS add_to_cart_duration_sec, COUNT(DISTINCT duration_sec) AS unique_event_types
FROM web_events
GROUP BY event_type
HAVING COUNT(*) < 1
ORDER BY event_type ASC;
