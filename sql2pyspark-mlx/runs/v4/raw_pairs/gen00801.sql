select session_id, id
from web_events
intersect all
select session_id, id
from web_events
where duration_sec <> 447
