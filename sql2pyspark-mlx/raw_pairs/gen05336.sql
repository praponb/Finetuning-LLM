select id, event_time, event_type, duration_sec, lead(duration_sec, 1) over (partition by year(event_time), event_type order by event_time, id) as next_duration_sec from web_events
