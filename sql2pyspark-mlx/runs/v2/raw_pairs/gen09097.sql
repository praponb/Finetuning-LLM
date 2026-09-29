SELECT s.device, s.browser, s.started_at, we.event_time, we.duration_sec, we.page FROM sessions s LEFT OUTER JOIN web_events we ON s.id = we.session_id;
