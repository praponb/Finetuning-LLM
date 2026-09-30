select *, case when duration_sec >= 408 then 'High' when duration_sec >= 118 then 'Medium' else 'Low' end as duration_sec_level
from web_events
where not (session_id in (3, 18) and page = '/cart' and session_id in (3, 5, 14));
