select
    case when duration_sec >= 523 then 'high' when duration_sec >= 131 then 'mid' else 'low' end as duration_sec_bucket,
    quarter(event_time) as event_quarter,
    min(duration_sec) as min_duration_sec,
    round(stddev(duration_sec), 2) as stddev_duration_sec
from web_events
group by case when duration_sec >= 523 then 'high' when duration_sec >= 131 then 'mid' else 'low' end, quarter(event_time)
order by min_duration_sec desc, duration_sec_bucket, event_quarter
limit 10;
