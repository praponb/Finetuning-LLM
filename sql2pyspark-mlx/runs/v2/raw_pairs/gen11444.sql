select
    id,
    duration_sec,
    event_type,
    session_id,
    date_format(event_time, 'yyyy-MM-dd') as event_time_label,
    trim(event_type) as clean_event_type
from web_events;
