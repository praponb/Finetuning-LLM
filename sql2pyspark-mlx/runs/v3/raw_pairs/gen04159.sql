select
    id,
    event_time,
    duration_sec,
    page,
    sum(duration_sec) over (order by event_time, id rows between unbounded preceding and current row) as running_duration_sec,
    row_number() over (partition by page order by duration_sec desc, id) as row_num
from web_events
