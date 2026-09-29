select
    id,
    event_type,
    duration_sec,
    event_time
from (
    select *, rank() over (partition by event_type order by duration_sec) as pos
    from web_events
    where to_date(event_time) < '2024-08-13'
) as t
where pos <= 3
