select sum(case when event_type = 'search' then 1 else 0 end) as search_count
from web_events
where duration_sec between 217 and 391 and (event_type in ('purchase', 'add_to_cart') or event_type = 'search')
order by search_count desc
