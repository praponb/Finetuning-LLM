SELECT s.id, s.device, s.browser, s.country FROM sessions s WHERE EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.event_type <> 'search') ORDER BY id DESC;
