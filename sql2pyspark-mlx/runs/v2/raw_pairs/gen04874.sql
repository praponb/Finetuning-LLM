SELECT MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range FROM web_events WHERE event_type IN ('page_view', 'search', 'add_to_cart')
