SELECT we.id, we.duration_sec, we.session_id, we.event_time FROM web_events we WHERE we.event_time >= '2024-09-01' AND we.session_id NOT IN (SELECT id FROM sessions WHERE browser = 'Safari');
