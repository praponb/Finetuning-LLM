SELECT sessions.country, sessions.device, web_events.duration_sec, web_events.page FROM sessions LEFT JOIN web_events ON sessions.id = web_events.session_id;
