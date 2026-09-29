SELECT s.id, s.browser, s.country
FROM sessions s
WHERE EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id)
ORDER BY s.id DESC, country;
