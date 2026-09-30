SELECT event_type, COUNT(CASE WHEN event_type = 'page_view' THEN 1 END) AS page_view_count FROM web_events GROUP BY event_type
