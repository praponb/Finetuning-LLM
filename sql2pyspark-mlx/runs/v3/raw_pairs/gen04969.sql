select
    date_trunc('MONTH', event_time) as event_month,
    sum(duration_sec) as total_duration_sec,
    count(*) as cnt,
    rank() over (order by sum(duration_sec) desc) as duration_sec_rank
from web_events
where session_id in (10, 11)
group by date_trunc('MONTH', event_time)
order by event_month desc;
