SELECT id, event_time, DATE_FORMAT(event_time, 'MMM yyyy') event_time_label, LOWER(event_type) event_type_lower FROM web_events WHERE event_type != 'add_to_cart' AND duration_sec BETWEEN 190 AND 263
