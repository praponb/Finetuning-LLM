select id, duration_sec, session_id
from web_events
where duration_sec >= (select avg(duration_sec) from web_events)
order by duration_sec desc, id
