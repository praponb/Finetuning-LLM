select
    id,
    event_type,
    duration_sec,
    dense_rank() over (partition by event_type order by duration_sec desc) as duration_sec_dense_rank
from web_events
