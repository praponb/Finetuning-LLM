select hour(event_time) as event_hour, to_date(event_time) as event_day, sum(case when page = '/search' then 1 else 0 end) as search_count
from web_events
group by hour(event_time), to_date(event_time)
having sum(case when page = '/search' then 1 else 0 end) >= 27
order by event_hour desc, event_day
limit 50;
