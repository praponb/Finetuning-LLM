SELECT we.id, we.duration_sec FROM web_events we WHERE we.duration_sec != 319 AND we.session_id NOT IN (SELECT id FROM sessions WHERE customer_id IN (12, 15, 28)) ORDER BY we.id
