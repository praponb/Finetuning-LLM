SELECT event_type, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range, COUNT(CASE WHEN page = '/cart' THEN 1 END) AS cart_count
FROM web_events
GROUP BY event_type
ORDER BY duration_sec_range ASC;
