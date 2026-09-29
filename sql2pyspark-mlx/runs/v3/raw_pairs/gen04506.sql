select id, event_type, duration_sec, min(duration_sec) over (partition by event_type) as event_type_min_duration_sec from web_events order by event_type_min_duration_sec desc, id desc
