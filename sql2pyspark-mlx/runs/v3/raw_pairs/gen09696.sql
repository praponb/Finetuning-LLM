SELECT s.device, s.country, we.page, DATE_FORMAT(s.started_at, 'yyyy-MM-dd') started_at_label FROM sessions s LEFT JOIN web_events we ON s.id = we.session_id WHERE we.duration_sec <= 460;
