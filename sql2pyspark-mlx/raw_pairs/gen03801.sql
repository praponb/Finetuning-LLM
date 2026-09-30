select
    id,
    event_type,
    session_id,
    duration_sec,
    max(duration_sec) over (partition by event_type, session_id) as event_type_max_duration_sec
from web_events
order by event_type_max_duration_sec desc
