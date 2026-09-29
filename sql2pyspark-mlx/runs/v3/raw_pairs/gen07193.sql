with top_web_events as (
    select *, rank() over (partition by session_id order by event_time) as pos
    from web_events
)
select
    id,
    session_id,
    event_time,
    duration_sec
from top_web_events
where pos <= 5
order by duration_sec desc;
