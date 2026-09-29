SELECT we.id, we.event_time, we.session_id
FROM web_events we
WHERE we.session_id IN (SELECT id FROM sessions WHERE customer_id = 25);
