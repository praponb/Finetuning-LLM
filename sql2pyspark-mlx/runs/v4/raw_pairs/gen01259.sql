with session_summary as (
    select session_id, count(duration_sec) as duration_sec_count
    from web_events
    group by session_id
)
select s.started_at, ss.duration_sec_count
from sessions s
left outer join session_summary ss on s.id = ss.session_id
order by started_at;
