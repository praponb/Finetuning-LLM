SELECT page, COUNT(CASE WHEN event_type = 'search' THEN 1 END) search_count FROM web_events WHERE HOUR(event_time) BETWEEN 10 AND 18 GROUP BY page ORDER BY page;
