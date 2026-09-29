select s.device, count(distinct we.duration_sec) as unique_event_types, avg(we.duration_sec) as avg_duration_sec from web_events we inner join sessions s on we.session_id = s.id group by s.device
