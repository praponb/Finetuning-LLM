select
    id,
    event_type,
    event_time,
    page,
    duration_sec,
    count(*) over (partition by event_type) as event_type_count,
    min(duration_sec) over (partition by date_trunc('month', event_time), page) as event_time_period_min_duration_sec
from web_events
