SELECT we.id, we.event_time, we.event_type, we.page FROM web_events we WHERE we.session_id IN (SELECT id FROM sessions WHERE customer_id IN (18, 31));
