SELECT we.id, we.duration_sec, s.device, s.browser, s.country FROM web_events we LEFT OUTER JOIN sessions s ON we.session_id = s.id ORDER BY country ASC, id ASC LIMIT 5;
