SELECT we.id, we.page
FROM web_events we
WHERE we.session_id IN (10, 14, 35) AND we.session_id IN (SELECT id FROM sessions WHERE device = 'mobile')
ORDER BY page ASC, we.id DESC;
