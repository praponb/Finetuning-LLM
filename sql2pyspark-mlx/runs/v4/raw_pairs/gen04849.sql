select id, event_time, duration_sec, lead(duration_sec, 2) over (order by event_time, id) as next_duration_sec from web_events order by next_duration_sec desc;
