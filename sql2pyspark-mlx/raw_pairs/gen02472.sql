select id, event_time, rank() over (order by event_time desc) as event_time_rank
from web_events
