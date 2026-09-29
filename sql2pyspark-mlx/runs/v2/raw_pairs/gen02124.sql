SELECT sessions.browser, sessions.customer_id, web_events.page
FROM sessions
LEFT OUTER JOIN web_events ON sessions.id = web_events.session_id
ORDER BY web_events.page
