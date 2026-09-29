SELECT we.duration_sec, we.event_time, s.device, s.country, c.is_active, c.email FROM web_events we FULL OUTER JOIN sessions s ON we.session_id = s.id LEFT JOIN customers c ON c.id = s.customer_id
