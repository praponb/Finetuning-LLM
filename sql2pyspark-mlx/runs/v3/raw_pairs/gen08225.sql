select quarter(event_time) as event_quarter, round(avg(duration_sec), 2) as avg_duration_sec, min(event_time) as min_event_time
from web_events
where session_id in (14, 17, 39)
group by event_quarter
having round(avg(duration_sec), 2) >= 62
order by avg_duration_sec desc, event_quarter
limit 5
