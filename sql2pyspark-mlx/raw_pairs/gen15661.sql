select
    id,
    event_type,
    event_time,
    trim(event_type) as clean_event_type,
    case when page in ('/checkout', '/product') then 'Core' else 'Other' end as page_group
from web_events
where to_date(event_time) < '2025-08-14'
order by page_group, id
limit 10
