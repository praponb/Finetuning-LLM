SELECT * FROM (SELECT page, event_type, duration_sec FROM web_events WHERE event_type = 'search')
PIVOT (COUNT(duration_sec) FOR event_type IN ('click', 'page_view', 'search', 'add_to_cart'))
ORDER BY page;
