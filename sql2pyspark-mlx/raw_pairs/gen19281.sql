select
    id,
    duration_sec,
    event_type,
    round(duration_sec, 1) as duration_sec_rounded
from web_events
order by duration_sec_rounded desc
