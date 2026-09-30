select event_time, case when duration_sec >= 437 then 'High' when duration_sec >= 61 then 'Medium' else 'Low' end as duration_sec_band
from web_events
where session_id in (29, 36)
order by event_time;
