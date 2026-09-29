select date_trunc('MONTH', event_time) as event_month, page, count(*) as num_web_events
from web_events
where duration_sec <= 268
group by date_trunc('MONTH', event_time), page
having max(duration_sec) > 320;
