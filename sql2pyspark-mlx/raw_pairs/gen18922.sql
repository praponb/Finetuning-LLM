SELECT DISTINCT page, event_type FROM web_events WHERE page IN ('/cart', '/home', '/search') ORDER BY event_type;
