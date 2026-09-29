select
    id,
    page,
    session_id,
    case when event_type = 'click' then 1 else 0 end is_click
from web_events
where event_type = 'purchase' and (duration_sec between 218 and 290 or page = '/checkout')
order by page;
