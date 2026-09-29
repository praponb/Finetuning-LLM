SELECT * FROM (SELECT page, event_type, duration_sec FROM web_events WHERE event_type = 'click') PIVOT (COUNT(duration_sec) FOR event_type IN ('click', 'add_to_cart', 'search', 'page_view'))
