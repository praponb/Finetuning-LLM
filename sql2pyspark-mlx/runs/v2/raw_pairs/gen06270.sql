SELECT page, SUM(CASE WHEN page = '/home' THEN 1 ELSE 0 END) AS home_count, SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list
FROM web_events
WHERE duration_sec BETWEEN 135 AND 363 AND page IN ('/search', '/home', '/checkout')
GROUP BY page
HAVING home_count < 53
ORDER BY home_count DESC;
