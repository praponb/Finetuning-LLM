SELECT we.id, we.session_id, we.event_time, we.page FROM web_events we WHERE we.session_id IN (SELECT id FROM sessions WHERE customer_id IN (23, 28, 35, 39))
