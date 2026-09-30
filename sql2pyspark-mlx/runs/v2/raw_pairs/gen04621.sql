with session_stats as (
    select session_id, round(avg(duration_sec), 2) as avg_duration_sec
    from web_events
    group by session_id
)
select s.started_at, ss.avg_duration_sec
from sessions s
join session_stats ss on s.id = ss.session_id
