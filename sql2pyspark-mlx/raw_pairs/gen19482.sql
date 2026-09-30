SELECT s.browser, s.device, s.country, we.page FROM sessions s LEFT JOIN web_events we ON we.session_id = s.id
