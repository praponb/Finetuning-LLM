SELECT we.duration_sec, s.browser FROM web_events we INNER JOIN sessions s ON we.session_id = s.id
