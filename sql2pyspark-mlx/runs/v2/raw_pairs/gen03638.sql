select
    id,
    page,
    event_time,
    duration_sec,
    lag(duration_sec, 1) over (partition by page order by event_time, id) as prev_duration_sec
from web_events
where session_id in (8, 13)
