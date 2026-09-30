select city
from stores
where city is not null
union all
select event_type
from web_events
where event_type is not null
order by city
