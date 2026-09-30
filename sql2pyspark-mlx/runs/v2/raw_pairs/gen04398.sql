SELECT id, event_time, page, event_type, HOUR(event_time) event_time_hour FROM web_events WHERE session_id IN (7, 24, 30, 32) AND event_type IN ('purchase', 'search', 'add_to_cart')
