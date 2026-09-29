select min(event_time) as first_event_time from web_events where to_date(event_time) < '2023-10-01' order by first_event_time
