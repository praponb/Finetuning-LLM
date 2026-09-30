select s.customer_id, s.browser, s.device, we.event_time, we.event_type, we.duration_sec from sessions s left join web_events we on we.session_id = s.id
