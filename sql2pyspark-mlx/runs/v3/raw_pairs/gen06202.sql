with session_stats as (
    select session_id, round(avg(duration_sec), 2) as avg_duration_sec
    from web_events
    where to_date(event_time) >= '2024-03-12'
    group by session_id
)
select s.customer_id, s.device, ss.avg_duration_sec
from sessions s
inner join session_stats ss on s.id = ss.session_id
where ss.avg_duration_sec >= 61
order by avg_duration_sec asc, customer_id asc;
