SELECT event_type, COUNT(CASE WHEN event_type = 'search' THEN 1 END) search_count FROM web_events GROUP BY event_type ORDER BY search_count DESC, event_type DESC LIMIT 1;
