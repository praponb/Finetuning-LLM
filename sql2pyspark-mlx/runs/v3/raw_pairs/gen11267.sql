SELECT s.started_at, we.page, we.event_time, we.duration_sec FROM sessions s INNER JOIN web_events we ON s.id = we.session_id WHERE we.session_id IN (7, 35) ORDER BY s.started_at;
