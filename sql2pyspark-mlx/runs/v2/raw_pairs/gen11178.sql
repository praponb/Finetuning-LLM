select quarter(event_time) as event_quarter, count(*) as num_web_events from web_events group by 1 order by num_web_events
