select id, session_id, trim(page) clean_page
from web_events
where event_type = 'purchase' and duration_sec < 304
order by id desc;
