select event_type, max(event_time) as latest_event_time, max(duration_sec) - min(duration_sec) as duration_sec_range
from web_events
where not (page = '/search' and (page = '/search' or session_id = 5))
group by event_type
order by latest_event_time desc, event_type asc;
