select id, duration_sec, page
from web_events
where duration_sec < (select min(duration_sec) from web_events where duration_sec <> 197);
