with top_web_events as (
    select
        id,
        session_id,
        event_time,
        duration_sec,
        row_number() over (partition by session_id order by event_time desc, id) as row_num
    from web_events
)
select
    id,
    session_id,
    event_time,
    duration_sec
from top_web_events
where row_num <= 5;
