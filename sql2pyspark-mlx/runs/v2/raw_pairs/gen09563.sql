select
    id,
    session_id,
    event_time,
    page
from (
    select *, row_number() over (partition by session_id order by event_time, id) as row_num
    from web_events
    where page <> '/home'
) as ranked
where row_num <= 1
