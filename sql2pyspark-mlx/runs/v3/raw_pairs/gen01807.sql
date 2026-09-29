select *, date_format(event_time, 'yyyy-MM') as event_time_label
from web_events;
