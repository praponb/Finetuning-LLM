select s.device, s.customer_id, we.session_id, we.duration_sec, we.event_time from sessions s left join web_events we on s.id = we.session_id order by we.session_id desc;
