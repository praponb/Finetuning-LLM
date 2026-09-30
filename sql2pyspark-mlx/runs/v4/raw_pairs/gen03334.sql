select
    id,
    session_id,
    event_type,
    page,
    event_time,
    duration_sec,
    avg(duration_sec) over (partition by page) as page_avg_duration_sec,
    ntile(10) over (order by duration_sec, id) as duration_sec_bucket
from web_events;
