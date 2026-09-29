SELECT id, page, CONCAT_WS(',', page, event_type) AS page_event_type_key FROM web_events WHERE event_time >= '2024-04-01' OR page NOT IN ('/home', '/checkout');
