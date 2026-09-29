SELECT we.id, we.event_time, we.session_id, s.customer_id, c.name, c.city FROM web_events we JOIN sessions s ON we.session_id = s.id JOIN customers c ON c.id = s.customer_id WHERE c.name IS NOT NULL;
