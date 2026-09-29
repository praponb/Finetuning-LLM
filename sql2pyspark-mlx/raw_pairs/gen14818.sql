with SessionSummary as (
    select session_id, avg(duration_sec) as avg_duration_sec, count(*) as total_count
    from web_events
    group by session_id
)
select
    s.id,
    s.device,
    S.avg_duration_sec,
    S.total_count
from sessions s
inner join SessionSummary S on s.id = S.session_id
