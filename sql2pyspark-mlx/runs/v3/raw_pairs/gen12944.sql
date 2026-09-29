SELECT page, MAX(duration_sec) AS max_duration_sec, SUM(CASE WHEN event_type = 'search' THEN 1 ELSE 0 END) AS search_count FROM web_events GROUP BY page HAVING max_duration_sec > 140;
