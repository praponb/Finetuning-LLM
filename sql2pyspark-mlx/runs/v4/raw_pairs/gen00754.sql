select
    event_type,
    avg(duration_sec) as avg_duration_sec,
    count(duration_sec) as duration_sec_count,
    max(event_time) as last_event_time
from web_events
where event_type = 'click'
group by event_type
having duration_sec_count < 3
order by avg_duration_sec desc;
