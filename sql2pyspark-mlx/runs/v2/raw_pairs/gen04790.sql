SELECT page FROM web_events WHERE page != '/cart' UNION ALL SELECT page FROM web_events WHERE page IN ('/checkout', '/home', '/cart');
