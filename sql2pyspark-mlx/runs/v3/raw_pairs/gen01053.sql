select
    id,
    session_id,
    event_type,
    event_time,
    ntile(2) over (partition by session_id, event_type order by event_time, id) bucket
from web_events
