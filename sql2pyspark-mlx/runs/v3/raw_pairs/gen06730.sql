SELECT s.id, s.country, s.browser
FROM sessions s
WHERE EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id)
ORDER BY s.browser ASC
