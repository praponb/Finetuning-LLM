select s.id, s.device, count(we.id) as web_event_count, avg(we.duration_sec) as avg_duration_sec from sessions s left join web_events we on s.id = we.session_id group by s.id, s.device
