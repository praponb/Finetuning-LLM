select id, session_id, page
from web_events
where event_type = 'click'
intersect
select id, session_id, page
from web_events
where event_time >= '2024-12-01'
order by page
