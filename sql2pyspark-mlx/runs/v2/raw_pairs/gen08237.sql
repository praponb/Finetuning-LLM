SELECT s.browser, we.duration_sec FROM sessions s INNER JOIN web_events we ON we.session_id = s.id
