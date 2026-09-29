select event_time, hour(event_time) as event_time_hour
from web_events
order by event_time;
