with numbered as (
    select *, row_number() over (partition by event_type order by duration_sec, id) as rn
    from web_events
)
select
    id,
    event_type,
    duration_sec,
    session_id
from numbered
where rn <= 2
