select id, event_type, duration_sec, avg(duration_sec) over (partition by event_type) as event_type_avg_duration_sec from web_events where duration_sec is not null;
