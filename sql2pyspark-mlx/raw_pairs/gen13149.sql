SELECT sessions.id, sessions.device, sessions.customer_id, sessions.browser FROM sessions WHERE NOT EXISTS (SELECT 1 FROM web_events WHERE web_events.session_id = sessions.id) ORDER BY 1 DESC
