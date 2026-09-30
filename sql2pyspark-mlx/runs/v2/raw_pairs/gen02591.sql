SELECT AVG(duration_sec) AS avg_duration_sec, COUNT(CASE WHEN page = '/cart' THEN 1 END) AS cart_count
FROM web_events
ORDER BY avg_duration_sec DESC;
