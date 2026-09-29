SELECT we.id, we.page FROM web_events we WHERE we.session_id NOT IN (SELECT id FROM sessions WHERE browser IN ('Chrome', 'Safari', 'Firefox'))
