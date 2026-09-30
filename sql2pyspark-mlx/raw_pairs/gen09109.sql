SELECT we.duration_sec, s.country, s.customer_id FROM web_events we RIGHT JOIN sessions s ON we.session_id = s.id ORDER BY s.customer_id DESC
