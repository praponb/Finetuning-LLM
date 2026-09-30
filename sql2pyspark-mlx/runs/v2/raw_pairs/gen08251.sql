select id, event_type
from web_events
where event_time < '2024-09-01'
union all
select id, event_type
from web_events
where duration_sec <> 356
order by event_type;
