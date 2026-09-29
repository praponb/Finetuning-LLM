select
    event_type,
    page,
    count(*) as web_event_count,
    max(duration_sec) as max_duration_sec,
    avg(duration_sec) as avg_duration_sec
from web_events
where to_date(event_time) < '2025-03-01'
group by event_type, page
order by max_duration_sec desc, event_type, page
limit 1;
