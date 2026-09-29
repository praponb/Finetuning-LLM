select
    id,
    event_time,
    concat_ws('/', page, event_type) as page_event_type_key,
    hour(event_time) as event_time_hour
from web_events
order by event_time
