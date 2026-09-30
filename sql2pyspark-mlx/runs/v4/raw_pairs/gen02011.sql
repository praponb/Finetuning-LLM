select month(event_time) as event_month, sum(duration_sec) as total_duration_sec, dense_rank() over (order by sum(duration_sec) desc) as duration_sec_rank
from web_events
group by month(event_time)
order by duration_sec_rank desc, total_duration_sec desc
