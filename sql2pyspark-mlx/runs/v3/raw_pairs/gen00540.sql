SELECT web_events.id, web_events.event_time
FROM web_events
WHERE HOUR(web_events.event_time) BETWEEN 11 AND 16 AND web_events.session_id NOT IN (SELECT id FROM sessions WHERE browser = 'Safari')
