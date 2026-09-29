SELECT s.id, s.device, s.country, s.browser FROM sessions s WHERE NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.duration_sec = 234)
