with session_summary as (
    select session_id, avg(duration_sec) as avg_duration_sec
    from web_events
    group by session_id
)
select sessions.customer_id, session_summary.avg_duration_sec
from sessions
inner join session_summary on sessions.id = session_summary.session_id
order by customer_id desc, avg_duration_sec desc;
