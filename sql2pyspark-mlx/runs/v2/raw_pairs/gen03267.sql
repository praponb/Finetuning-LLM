SELECT we.id, we.session_id FROM web_events we WHERE we.session_id IN (SELECT id FROM sessions WHERE customer_id IS NOT NULL);
