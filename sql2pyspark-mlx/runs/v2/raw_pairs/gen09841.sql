SELECT we.id, we.page, we.event_time, we.duration_sec FROM web_events we WHERE we.session_id NOT IN (SELECT id FROM sessions WHERE customer_id IS NULL) ORDER BY we.id ASC
