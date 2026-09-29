with ranked_web_events as (
    select
        id,
        event_type,
        event_time,
        duration_sec,
        row_number() over (partition by event_type order by event_time desc, id) as rn
    from web_events
)
select
    id,
    event_type,
    event_time,
    duration_sec
from ranked_web_events
where rn = 1
order by event_type desc
