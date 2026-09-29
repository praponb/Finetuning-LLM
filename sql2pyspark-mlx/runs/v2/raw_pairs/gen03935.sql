select
    id,
    event_time,
    page,
    initcap(trim(event_type)) as event_type_title
from web_events
