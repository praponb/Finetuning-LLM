SELECT event_time, session_id, event_type FROM web_events WHERE page = '/product' AND event_type NOT IN ('add_to_cart', 'click', 'search');
