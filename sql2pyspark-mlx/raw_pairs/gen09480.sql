select id, event_time, dense_rank() over (partition by year(event_time) order by event_time) event_time_dense_rank
from web_events;
