select event_type, date_trunc('MONTH', event_time) as event_month, count(duration_sec) as duration_sec_count
from web_events
group by event_type, date_trunc('MONTH', event_time)
having duration_sec_count >= 5
order by event_month, event_type
limit 50
