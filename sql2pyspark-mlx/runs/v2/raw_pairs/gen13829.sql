select id, event_type, event_time, duration_sec, duration_sec - lag(duration_sec, 1) over (partition by event_type order by event_time, id) duration_sec_change from web_events;
