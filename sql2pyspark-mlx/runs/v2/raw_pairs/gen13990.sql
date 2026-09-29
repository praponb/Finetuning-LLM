SELECT s.id, s.customer_id, s.device, s.started_at FROM sessions s WHERE EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.event_type = 'search') ORDER BY started_at, id LIMIT 5
