select event_type, round(avg(duration_sec), 2) as avg_duration_sec
from web_events
where hour(event_time) between 7 and 15
group by event_type
