select *
from (
    select *, row_number() over (partition by page order by event_time, id) as rn
    from web_events
) as ranked
where rn <= 2;
