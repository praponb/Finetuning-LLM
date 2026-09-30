SELECT we.id, we.session_id, we.event_time
FROM web_events we
WHERE we.session_id IN (7, 18, 40) AND we.session_id NOT IN (SELECT id FROM sessions WHERE device = 'mobile');
