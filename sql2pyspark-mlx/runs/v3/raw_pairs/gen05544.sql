select page, duration_sec
from web_events
where duration_sec <= 203
union
select page, duration_sec
from web_events
where duration_sec >= 471
order by duration_sec;
