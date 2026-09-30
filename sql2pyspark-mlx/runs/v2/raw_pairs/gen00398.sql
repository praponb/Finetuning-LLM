select event_type, page, min(duration_sec) as min_duration_sec from web_events group by cube(event_type, page) order by min_duration_sec desc;
