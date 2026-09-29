select id, duration_sec
from web_events
where hour(event_time) between 12 and 16
union all
select id, duration_sec
from web_events
where event_type in ('add_to_cart', 'page_view');
