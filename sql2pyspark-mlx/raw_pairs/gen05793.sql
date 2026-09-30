select duration_sec, to_date(event_time) as event_time_date
from web_events
where event_type = 'purchase';
