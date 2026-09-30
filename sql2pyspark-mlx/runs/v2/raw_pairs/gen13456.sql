select id, duration_sec
from web_events
where hour(event_time) between 7 and 13
union
select id, duration_sec
from web_events
where event_type in ('search', 'purchase')
order by duration_sec
