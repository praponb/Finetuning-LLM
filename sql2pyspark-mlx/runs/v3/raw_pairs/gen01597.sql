SELECT s.device, s.browser, s.country, we.duration_sec, we.event_time FROM sessions s LEFT OUTER JOIN web_events we ON we.session_id = s.id;
