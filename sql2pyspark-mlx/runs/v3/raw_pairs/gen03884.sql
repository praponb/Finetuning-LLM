SELECT we.id, we.session_id
FROM web_events we
WHERE we.session_id NOT IN (SELECT id FROM sessions WHERE customer_id IS NULL)
ORDER BY we.session_id DESC
