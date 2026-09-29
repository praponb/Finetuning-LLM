SELECT we.id, we.duration_sec, we.event_type, we.page FROM web_events we WHERE we.session_id NOT IN (SELECT id FROM sessions WHERE device = 'desktop') ORDER BY duration_sec DESC
