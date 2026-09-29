SELECT s.browser, s.device, we.session_id FROM sessions s LEFT JOIN web_events we ON we.session_id = s.id;
