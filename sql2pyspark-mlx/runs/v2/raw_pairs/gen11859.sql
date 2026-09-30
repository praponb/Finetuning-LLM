SELECT id, duration_sec FROM web_events WHERE page IN ('/search', '/home', '/product') AND duration_sec >= 430;
