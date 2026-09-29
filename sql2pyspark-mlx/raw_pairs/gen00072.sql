SELECT we.duration_sec, we.event_type, we.page, s.browser, s.device FROM web_events we INNER JOIN sessions s ON we.session_id = s.id WHERE we.session_id IN (6, 40);
