select duration_sec, session_id, cast(event_time as date) as event_time_day
from web_events
where duration_sec < 291 and session_id = 15
order by event_time_day;
