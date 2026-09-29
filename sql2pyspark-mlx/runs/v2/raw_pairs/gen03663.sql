SELECT s.id, s.country FROM sessions s WHERE s.id IN (SELECT session_id FROM web_events WHERE event_type = 'add_to_cart');
