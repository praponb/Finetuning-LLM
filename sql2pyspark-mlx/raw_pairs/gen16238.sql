with Numbered as (
    select
        id,
        page,
        event_time,
        event_type,
        row_number() over (partition by page order by event_time, id) as pos
    from web_events
)
select
    id,
    page,
    event_time,
    event_type
from Numbered
where pos = 1
order by id, event_type;
