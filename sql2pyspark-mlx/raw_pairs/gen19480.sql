SELECT event_type, 'web_event' AS record_type FROM web_events WHERE event_type IS NOT NULL UNION SELECT status, 'order' AS record_type FROM orders WHERE status IS NOT NULL;
