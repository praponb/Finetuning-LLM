SELECT s.started_at, we.session_id FROM sessions s LEFT OUTER JOIN web_events we ON we.session_id = s.id;
