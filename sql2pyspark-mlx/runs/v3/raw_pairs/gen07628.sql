select
    id,
    event_type,
    duration_sec,
    ntile(10) over (partition by event_type order by duration_sec desc, id) duration_sec_ntile
from web_events
