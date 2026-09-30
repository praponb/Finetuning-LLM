select
    event_type,
    page,
    avg(duration_sec) as avg_duration_sec,
    count(case when page = '/home' then 1 end) as home_count
from web_events
group by event_type, page
order by page, avg_duration_sec desc
