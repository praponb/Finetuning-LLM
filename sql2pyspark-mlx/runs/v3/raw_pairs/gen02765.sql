SELECT we.id, we.event_type, we.page, we.duration_sec FROM web_events we WHERE we.duration_sec IS NOT NULL AND we.session_id NOT IN (SELECT id FROM sessions WHERE customer_id IS NULL);
