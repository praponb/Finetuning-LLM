select
    id,
    duration_sec,
    session_id,
    upper(event_type) as event_type_upper
from web_events
where session_id in (4, 16, 39, 40) and (event_type = 'search' or session_id in (8, 18, 29))
order by id, session_id desc
