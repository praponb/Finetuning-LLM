SELECT we.id, we.session_id
FROM web_events we
WHERE we.session_id NOT IN (SELECT id FROM sessions WHERE country = 'SG')
ORDER BY id DESC;
