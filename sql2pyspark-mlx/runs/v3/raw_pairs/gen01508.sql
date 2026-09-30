SELECT we.event_time, s.customer_id, s.device, s.started_at FROM web_events we JOIN sessions s ON we.session_id = s.id WHERE we.session_id = 21 ORDER BY we.event_time ASC, s.device DESC;
