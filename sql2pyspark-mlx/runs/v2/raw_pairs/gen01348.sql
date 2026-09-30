SELECT
    s.id,
    s.browser,
    s.started_at,
    s.device
FROM sessions s
WHERE EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.page IN ('/cart', '/search', '/checkout'))
ORDER BY device DESC
