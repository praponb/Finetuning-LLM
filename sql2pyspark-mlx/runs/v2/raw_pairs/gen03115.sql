SELECT s.country, we.session_id, we.page
FROM sessions s
LEFT JOIN web_events we ON we.session_id = s.id
WHERE s.browser IN ('Edge', 'Firefox')
ORDER BY page;
