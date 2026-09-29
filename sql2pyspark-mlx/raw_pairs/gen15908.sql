SELECT s.country, we.session_id, we.event_time, we.page FROM sessions s INNER JOIN web_events we ON we.session_id = s.id ORDER BY we.session_id DESC
