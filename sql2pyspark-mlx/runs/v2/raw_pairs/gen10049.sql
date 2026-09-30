SELECT we.id, we.session_id
FROM web_events we
WHERE we.page IN ('/home', '/search') AND we.session_id IN (SELECT id FROM sessions WHERE browser != 'Firefox')
ORDER BY id ASC, we.session_id DESC
LIMIT 3;
