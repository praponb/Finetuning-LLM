SELECT sessions.country, sessions.device, web_events.duration_sec
FROM sessions
LEFT JOIN web_events ON sessions.id = web_events.session_id
ORDER BY duration_sec ASC
