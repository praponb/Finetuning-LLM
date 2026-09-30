select
    event_type,
    session_id,
    hour(event_time) as event_time_hour,
    case when event_type = 'purchase' then 1 else 0 end as is_purchase
from web_events
order by event_time_hour desc;
