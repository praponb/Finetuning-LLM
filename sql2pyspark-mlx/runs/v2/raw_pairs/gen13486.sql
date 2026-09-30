SELECT we.id, we.page, we.event_type, we.session_id FROM web_events we WHERE we.session_id NOT IN (SELECT id FROM sessions WHERE browser NOT IN ('Chrome', 'Edge', 'Firefox'));
