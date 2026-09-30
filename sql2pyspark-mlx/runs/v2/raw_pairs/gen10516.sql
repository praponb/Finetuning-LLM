SELECT we.id, we.event_time, we.session_id, s.country, s.device, c.name FROM web_events we FULL OUTER JOIN sessions s ON we.session_id = s.id LEFT JOIN customers c ON c.id = s.customer_id;
