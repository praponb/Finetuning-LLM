SELECT we.id, we.page, we.session_id
FROM web_events we
WHERE we.session_id NOT IN (SELECT id FROM sessions WHERE customer_id IN (8, 11, 19, 34))
