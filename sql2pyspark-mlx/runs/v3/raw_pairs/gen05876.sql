select
    page,
    max(event_time) as latest_event_time,
    sum(duration_sec) as total_duration_sec,
    avg(duration_sec) as avg_duration_sec
from web_events
where page = '/product' and page in ('/home', '/product', '/search') and event_time >= '2024-07-01'
group by page
having sum(duration_sec) > 900
order by latest_event_time desc, page
limit 10;
