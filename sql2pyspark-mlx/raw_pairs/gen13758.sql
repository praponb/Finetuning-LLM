select id, duration_sec
from web_events
where duration_sec > (select max(duration_sec) from web_events);
