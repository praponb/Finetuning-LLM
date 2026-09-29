select *
from web_events
where duration_sec >= 232 and duration_sec < 336
order by duration_sec desc;
