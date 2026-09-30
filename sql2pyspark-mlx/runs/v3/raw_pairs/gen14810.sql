select event_type, sum(duration_sec) as total_duration_sec, rank() over (order by sum(duration_sec) desc) as rnk
from web_events
where duration_sec between 382 and 474
group by event_type
order by event_type desc;
