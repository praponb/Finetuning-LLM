SELECT we.session_id, we.event_type, s.country, s.customer_id, s.started_at FROM web_events we JOIN sessions s ON we.session_id = s.id ORDER BY s.started_at
