SELECT web_events.id, web_events.duration_sec, sessions.country
FROM web_events
INNER JOIN sessions ON web_events.session_id = sessions.id
ORDER BY duration_sec DESC, id
LIMIT 5;
