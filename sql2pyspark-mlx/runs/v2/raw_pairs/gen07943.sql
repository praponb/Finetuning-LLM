SELECT page, SUM(CASE WHEN event_type = 'search' THEN duration_sec ELSE 0 END) AS search_duration_sec, SUM(duration_sec) AS total_duration_sec
FROM web_events
GROUP BY page
ORDER BY total_duration_sec DESC, page
LIMIT 20
