select
    duration_sec,
    event_type,
    page,
    case when duration_sec >= 449 then 'High' when duration_sec >= 208 then 'Medium' else 'Low' end as duration_sec_band
from web_events
where session_id in (10, 20, 28) and (to_date(event_time) >= '2024-08-01' or event_type = 'click')
order by page desc
