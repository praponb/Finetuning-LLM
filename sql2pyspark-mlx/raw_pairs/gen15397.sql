select
    event_type,
    page,
    min(duration_sec) as min_duration_sec,
    count(case when event_type = 'add_to_cart' then 1 end) as add_to_cart_count,
    count(distinct duration_sec) as unique_event_types
from web_events
where event_type != 'add_to_cart'
group by event_type, page
having add_to_cart_count < 3
order by page, event_type
limit 5
