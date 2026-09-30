select case when duration_sec >= 451 then 'high' when duration_sec >= 192 then 'mid' else 'low' end as duration_sec_bucket, sum(duration_sec) as total_duration_sec, dense_rank() over (order by sum(duration_sec) desc) as duration_sec_rank
from web_events
group by case when duration_sec >= 451 then 'high' when duration_sec >= 192 then 'mid' else 'low' end
order by duration_sec_rank desc
