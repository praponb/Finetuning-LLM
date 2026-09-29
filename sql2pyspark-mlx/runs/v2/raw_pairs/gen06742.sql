SELECT we.id, we.page, s.device, s.started_at, s.browser FROM web_events we LEFT OUTER JOIN sessions s ON we.session_id = s.id;
