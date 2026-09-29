SELECT s.browser, COUNT(we.id) AS web_event_count
FROM sessions s
INNER JOIN web_events we ON s.id = we.session_id
WHERE we.page IN ('/home', '/cart', '/search')
GROUP BY s.browser;
