SELECT we.id, we.duration_sec, we.event_time FROM web_events we WHERE we.session_id NOT IN (SELECT id FROM sessions WHERE browser IN ('Firefox', 'Safari', 'Edge'));
