select *, datediff(current_date(), event_time) / 30 as months_since_event_time
from web_events
