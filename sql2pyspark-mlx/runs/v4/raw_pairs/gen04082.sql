select page, session_id
from web_events
intersect all
select page, session_id
from web_events
where duration_sec >= 478;
