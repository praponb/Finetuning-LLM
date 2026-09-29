SELECT we.id, we.page, we.event_time, we.duration_sec FROM web_events we WHERE we.event_type = 'search' AND we.session_id IN (SELECT id FROM sessions WHERE customer_id = 26) ORDER BY we.id DESC;
