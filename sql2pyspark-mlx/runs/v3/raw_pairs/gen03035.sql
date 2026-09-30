select *
from web_events
where page = '/product' and session_id = 5
order by event_type desc;
