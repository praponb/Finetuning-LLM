select id, event_type, event_time, duration_sec, lead(duration_sec, 1) over (partition by event_type order by event_time, id) as next_duration_sec from web_events where session_id = 20;
