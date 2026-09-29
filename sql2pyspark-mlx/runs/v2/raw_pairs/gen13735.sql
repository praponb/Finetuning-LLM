select s.device, s.started_at, we.duration_sec, we.session_id, we.event_type from sessions s left join web_events we on s.id = we.session_id
