with session_summary as (
    select session_id, max(duration_sec) max_duration_sec
    from web_events
    where session_id = 1
    group by session_id
)
select s.id, s.device, ss.max_duration_sec
from sessions s
left join session_summary ss on s.id = ss.session_id
where ss.max_duration_sec >= 320;
