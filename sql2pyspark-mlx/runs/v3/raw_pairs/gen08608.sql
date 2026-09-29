select
    id,
    page,
    event_time,
    duration_sec,
    sum(duration_sec) over (partition by page order by event_time, id) running_duration_sec
from web_events;
