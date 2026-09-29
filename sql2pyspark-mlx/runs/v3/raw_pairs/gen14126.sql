SELECT we.id, we.session_id, we.page, we.event_time FROM web_events we WHERE we.session_id IN (SELECT id FROM sessions WHERE browser IN ('Safari', 'Firefox', 'Chrome'))
