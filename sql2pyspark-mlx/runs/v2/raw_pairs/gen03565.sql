SELECT sessions.id, sessions.customer_id, sessions.started_at
FROM sessions
WHERE EXISTS (SELECT 1 FROM web_events WHERE web_events.session_id = sessions.id)
ORDER BY id, sessions.customer_id;
