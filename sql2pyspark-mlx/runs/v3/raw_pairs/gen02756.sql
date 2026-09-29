select
    id,
    event_time,
    session_id,
    floor(duration_sec) as duration_sec_whole,
    case when duration_sec >= 377 then 'High' when duration_sec >= 236 then 'Medium' else 'Low' end as duration_sec_band
from web_events
where page not in ('/cart', '/search');
