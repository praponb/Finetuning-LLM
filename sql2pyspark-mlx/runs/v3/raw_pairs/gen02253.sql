select quarter(event_time) as event_quarter, date_format(event_time, 'yyyy-MM') as event_month, max(duration_sec) - min(duration_sec) as duration_sec_range
from web_events
group by 1, 2;
