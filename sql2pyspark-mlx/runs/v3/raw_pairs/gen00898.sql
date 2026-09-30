SELECT CASE WHEN page IN ('/home', '/search') THEN 'priority' ELSE 'other' END AS page_group, SUM(duration_sec) AS total_duration_sec, SUM(SUM(duration_sec)) OVER (ORDER BY CASE WHEN page IN ('/home', '/search') THEN 'priority' ELSE 'other' END) AS cumulative_duration_sec
FROM web_events
GROUP BY CASE WHEN page IN ('/home', '/search') THEN 'priority' ELSE 'other' END
ORDER BY total_duration_sec, page_group
LIMIT 10
