with ranked as (
    select *, rank() over (partition by page order by event_time desc) as dedup_rank
    from web_events
    qualify dedup_rank = 1
)
select
    id,
    session_id,
    page,
    event_time,
    duration_sec,
    dedup_rank
from ranked
