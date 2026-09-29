select
    id,
    session_id,
    event_time,
    ntile(10) over (partition by session_id order by event_time, id) quartile
from web_events
order by quartile desc
