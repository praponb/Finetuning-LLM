SELECT we.id, we.event_type, we.event_time, we.duration_sec FROM web_events we WHERE we.session_id IN (SELECT id FROM sessions WHERE browser = 'Firefox');
