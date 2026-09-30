with ranked as (
    select *, rank() over (partition by event_type order by duration_sec desc) pos
    from web_events
)
select
    id,
    event_type,
    duration_sec,
    event_time
from ranked
where pos <= 3;
