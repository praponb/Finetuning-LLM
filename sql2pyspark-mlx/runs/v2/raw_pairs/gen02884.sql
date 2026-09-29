select
    id,
    event_type,
    page,
    event_time,
    case when event_type = 'search' then 1 else 0 end as is_search
from web_events
where page in ('/cart', '/product', '/checkout') or event_time < '2023-11-26'
