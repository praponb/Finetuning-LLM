SELECT we.id, we.page
FROM web_events we
WHERE we.duration_sec >= 352 AND we.session_id IN (SELECT id FROM sessions WHERE customer_id IS NULL)
