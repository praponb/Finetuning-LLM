select
    id,
    page,
    event_time,
    duration_sec,
    session_id,
    lead(duration_sec, 1) over (partition by page order by event_time, id) as next_duration_sec,
    avg(duration_sec) over (partition by session_id) as session_id_avg_duration_sec
from web_events
where page = '/checkout'
order by duration_sec desc, event_time desc
