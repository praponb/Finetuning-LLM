select s.device, s.started_at, we.page, length(s.device) as device_length from sessions s left outer join web_events we on we.session_id = s.id where we.session_id in (1, 5, 19, 35);
