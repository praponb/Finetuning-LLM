SELECT sessions.id, sessions.browser, sessions.country
FROM sessions
WHERE EXISTS (SELECT 1 FROM web_events WHERE web_events.session_id = sessions.id)
ORDER BY sessions.id DESC
LIMIT 1;
