SELECT we.id, we.session_id, we.duration_sec
FROM web_events we
WHERE we.duration_sec <> 261 AND we.session_id NOT IN (SELECT id FROM sessions WHERE customer_id IS NOT NULL);
