select month(event_time) event_month, count(distinct duration_sec) unique_event_types from web_events group by month(event_time)
