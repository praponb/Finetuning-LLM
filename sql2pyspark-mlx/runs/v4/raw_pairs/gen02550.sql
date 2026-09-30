SELECT page, event_type FROM web_events WHERE page != '/checkout' UNION ALL SELECT page, event_type FROM web_events WHERE page = '/cart' ORDER BY event_type;
