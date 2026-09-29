SELECT we.id, we.event_time, we.event_type, we.page, s.browser FROM web_events we JOIN sessions s ON we.session_id = s.id ORDER BY we.id DESC LIMIT 50;
