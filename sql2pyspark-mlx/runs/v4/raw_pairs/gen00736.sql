SELECT session_id, event_type FROM web_events WHERE event_type = 'page_view' UNION SELECT session_id, event_type FROM web_events WHERE event_type = 'add_to_cart';
