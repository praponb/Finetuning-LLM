SELECT we.id, we.event_time, we.session_id, s.country, s.customer_id FROM web_events we RIGHT OUTER JOIN sessions s ON we.session_id = s.id;
