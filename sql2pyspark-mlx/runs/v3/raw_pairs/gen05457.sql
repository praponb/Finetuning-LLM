select hour(event_time) as event_hour, sum(duration_sec) as total_duration_sec, count(case when event_type = 'click' then 1 end) as click_count from web_events group by hour(event_time)
