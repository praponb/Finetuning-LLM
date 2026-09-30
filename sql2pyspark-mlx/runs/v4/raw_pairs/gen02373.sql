select
    id,
    session_id,
    event_type,
    page,
    event_time,
    duration_sec,
    sum(duration_sec) over (partition by event_type order by event_time, id rows between unbounded preceding and current row) as running_duration_sec,
    sum(duration_sec) over (order by event_time, id rows between unbounded preceding and current row) as running_duration_sec_2
from web_events
