select id, duration_sec
from web_events
where duration_sec < 135
union all
select id, duration_sec
from web_events
where event_type = 'search';
