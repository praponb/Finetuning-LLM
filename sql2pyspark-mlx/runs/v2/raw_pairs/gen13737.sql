SELECT we.id, we.event_type, s.device, c.is_active FROM web_events we LEFT JOIN sessions s ON we.session_id = s.id LEFT JOIN customers c ON s.customer_id = c.id
