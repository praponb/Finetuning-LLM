SELECT id, duration_sec FROM web_events WHERE event_type = 'add_to_cart' UNION SELECT id, duration_sec FROM web_events WHERE event_time < '2023-06-01'
