SELECT sessions.id, sessions.device, COUNT(web_events.id) AS web_event_count FROM sessions LEFT OUTER JOIN web_events ON sessions.id = web_events.session_id GROUP BY sessions.id, sessions.device
