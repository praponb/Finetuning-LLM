with session_stats as (
    select session_id, min(duration_sec) as min_duration_sec
    from web_events
    where event_time >= '2023-08-01'
    group by session_id
)
select
    s.id,
    s.browser,
    s.started_at,
    ss.min_duration_sec
from sessions s
left outer join session_stats ss on s.id = ss.session_id
order by started_at desc, s.browser
