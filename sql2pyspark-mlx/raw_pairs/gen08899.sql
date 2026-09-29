SELECT CASE WHEN page IN ('/checkout', '/search') THEN 'core' ELSE 'other' END AS page_group, ROUND(AVG(duration_sec), 2) AS avg_duration_sec, COUNT(*) AS total_count
FROM web_events
GROUP BY page_group
ORDER BY page_group;
