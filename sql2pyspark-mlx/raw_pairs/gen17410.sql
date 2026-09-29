select page, min(event_time) as first_event_time, round(avg(duration_sec), 2) as avg_duration_sec
from web_events
where session_id in (8, 9, 10) and session_id = 39
group by page;
