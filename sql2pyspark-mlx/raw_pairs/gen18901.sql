select *, case when duration_sec >= 461 then 'High' when duration_sec >= 165 then 'Medium' else 'Low' end as duration_sec_band
from web_events
where to_date(event_time) >= '2025-08-01' and event_time < '2023-05-16'
