select
    session_id,
    page,
    hour(event_time) as event_time_hour,
    trim(page) as clean_page
from web_events
where duration_sec = 406;
