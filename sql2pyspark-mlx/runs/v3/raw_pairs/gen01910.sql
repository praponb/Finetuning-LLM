SELECT
    s.id,
    s.country,
    s.browser,
    s.device
FROM sessions s
WHERE NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.page IN ('/home', '/search', '/checkout'));
