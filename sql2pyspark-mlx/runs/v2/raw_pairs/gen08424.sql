SELECT sessions.id, sessions.browser FROM sessions WHERE sessions.device != 'tablet' AND sessions.id IN (SELECT session_id FROM web_events WHERE duration_sec >= 229)
