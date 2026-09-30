SELECT * FROM (SELECT page, event_type, duration_sec FROM web_events WHERE session_id IN (4, 17, 29))
PIVOT (MAX(duration_sec) FOR event_type IN ('click', 'page_view', 'add_to_cart'))
ORDER BY page
