select
    id,
    duration_sec,
    session_id,
    event_time
from web_events
where duration_sec < (select min(duration_sec) from web_events where page in ('/cart', '/search'))
order by duration_sec desc;
