SELECT event_type, COUNT(*) AS total_count, SUM(CASE WHEN page = '/home' THEN duration_sec ELSE 0 END) AS home_duration_sec
FROM web_events
WHERE page IN ('/home', '/cart')
GROUP BY event_type
HAVING SUM(CASE WHEN page = '/home' THEN duration_sec ELSE 0 END) > 54;
