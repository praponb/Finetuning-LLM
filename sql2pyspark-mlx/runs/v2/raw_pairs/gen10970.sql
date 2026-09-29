select *, to_date(event_time) as event_time_date
from web_events
where not (event_type = 'add_to_cart' and page is not null)
order by event_time_date, id
limit 10;
