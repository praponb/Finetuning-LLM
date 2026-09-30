SELECT web_events.id, web_events.duration_sec
FROM web_events
WHERE web_events.duration_sec > 127 AND web_events.session_id IN (SELECT id FROM sessions WHERE device IN ('tablet', 'mobile'))
ORDER BY duration_sec DESC, id;
