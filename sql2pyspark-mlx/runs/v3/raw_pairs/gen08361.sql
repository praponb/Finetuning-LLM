select max(duration_sec) - min(duration_sec) duration_sec_range
from web_events
where duration_sec between 213 and 238
order by duration_sec_range desc
