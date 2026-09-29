select id, event_type, event_time, duration_sec, sum(duration_sec) over (partition by event_type order by event_time, id) as running_duration_sec from web_events;
