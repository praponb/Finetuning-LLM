SELECT id FROM web_events EXCEPT SELECT id FROM web_events WHERE page NOT IN ('/product', '/checkout', '/home');
