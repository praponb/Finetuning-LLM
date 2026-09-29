SELECT session_id FROM web_events WHERE event_type != 'page_view' UNION SELECT session_id FROM web_events WHERE event_type IN ('search', 'click', 'add_to_cart');
