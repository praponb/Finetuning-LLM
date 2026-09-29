SELECT s.browser, we.duration_sec, we.session_id FROM sessions s INNER JOIN web_events we ON we.session_id = s.id WHERE s.country = 'DE';
