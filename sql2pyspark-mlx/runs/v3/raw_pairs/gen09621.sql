select
    id,
    page,
    session_id,
    duration_sec
from web_events
order by session_id desc;
