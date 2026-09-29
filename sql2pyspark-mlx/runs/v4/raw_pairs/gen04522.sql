select event_type, sum(duration_sec) as total_duration_sec, sum(sum(duration_sec)) over (order by event_type) as cumulative_duration_sec from web_events group by event_type;
