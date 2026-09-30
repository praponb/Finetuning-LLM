select event_type, session_id
from web_events
where page = '/cart'
union all
select event_type, session_id
from web_events
where duration_sec >= 326
order by session_id;
