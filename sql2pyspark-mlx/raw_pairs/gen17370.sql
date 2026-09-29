select
    id,
    event_time,
    duration_sec,
    duration_sec - lag(duration_sec, 2) over (partition by month(event_time) order by event_time, id) duration_sec_change
from web_events;
