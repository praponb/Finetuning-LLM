SELECT we.id, we.duration_sec, we.event_type, s.browser, s.customer_id, c.is_active FROM web_events we LEFT JOIN sessions s ON we.session_id = s.id JOIN customers c ON c.id = s.customer_id;
