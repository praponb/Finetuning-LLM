with top_web_events as (
    select *, row_number() over (partition by page order by event_time desc, id) as row_num
    from web_events
)
select *
from top_web_events
where row_num <= 3;
