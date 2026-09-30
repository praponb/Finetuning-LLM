SELECT we.event_time, s.country, c.is_active FROM web_events we LEFT OUTER JOIN sessions s ON we.session_id = s.id LEFT OUTER JOIN customers c ON s.customer_id = c.id ORDER BY we.event_time;
