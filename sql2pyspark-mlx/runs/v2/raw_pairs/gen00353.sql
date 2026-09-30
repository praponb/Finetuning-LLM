SELECT * FROM (SELECT page, event_type, duration_sec FROM web_events WHERE session_id IN (9, 28, 40))
PIVOT (MAX(duration_sec) FOR event_type IN ('search', 'page_view', 'click', 'add_to_cart'))
ORDER BY page
