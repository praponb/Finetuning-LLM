select *
from web_events
where not (event_time >= '2023-06-01' and session_id in (28, 34, 37))
order by event_time desc;
