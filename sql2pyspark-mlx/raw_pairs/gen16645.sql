SELECT sessions.id, sessions.started_at
FROM sessions
WHERE EXISTS (SELECT 1 FROM web_events WHERE web_events.session_id = sessions.id);
