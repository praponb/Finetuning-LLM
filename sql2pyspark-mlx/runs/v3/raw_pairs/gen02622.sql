SELECT we.id, we.event_time FROM web_events we WHERE we.session_id IN (SELECT id FROM sessions WHERE browser = 'Chrome') ORDER BY id LIMIT 20;
