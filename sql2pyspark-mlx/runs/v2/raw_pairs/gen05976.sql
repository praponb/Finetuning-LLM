select year(event_time) as event_year, event_type, round(avg(duration_sec), 2) as avg_duration_sec from web_events group by year(event_time), event_type having round(avg(duration_sec), 2) >= 27;
