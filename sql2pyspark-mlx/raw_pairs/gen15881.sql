SELECT page, event_type, event_time FROM web_events WHERE page NOT IN ('/product', '/search', '/checkout')
