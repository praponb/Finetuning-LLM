SELECT we.id, we.event_type, we.event_time FROM web_events we WHERE we.session_id NOT IN (SELECT id FROM sessions WHERE customer_id IN (10, 32, 36));
