select
    page,
    event_type,
    count(*) as num_web_events,
    max(duration_sec) as max_duration_sec
from web_events
group by page, event_type;
