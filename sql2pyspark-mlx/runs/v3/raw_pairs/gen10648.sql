SELECT id, event_type, event_time, duration_sec, SUBSTRING(event_type, 1, 3) event_type_prefix FROM web_events WHERE event_type IN ('add_to_cart', 'search', 'purchase') OR duration_sec < 340
