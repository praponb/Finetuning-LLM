select quarter(event_time) as event_quarter, event_type, count(distinct duration_sec) as unique_sessions
from web_events
where page = '/product'
group by quarter(event_time), event_type;
