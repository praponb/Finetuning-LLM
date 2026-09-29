select
    id,
    session_id,
    duration_sec,
    event_time
from (
    select *, row_number() over (partition by session_id order by duration_sec, id) as row_num
    from web_events
) as ranked
where row_num <= 5
order by id desc;
