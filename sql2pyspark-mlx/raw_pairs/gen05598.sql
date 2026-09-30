SELECT page, event_type FROM web_events WHERE duration_sec > 350 UNION SELECT page, event_type FROM web_events WHERE page IN ('/product', '/search', '/cart');
