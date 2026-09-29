SELECT s.browser, we.event_type, we.page
FROM sessions s
LEFT OUTER JOIN web_events we ON we.session_id = s.id
WHERE we.session_id IN (16, 22, 30)
ORDER BY event_type DESC;
