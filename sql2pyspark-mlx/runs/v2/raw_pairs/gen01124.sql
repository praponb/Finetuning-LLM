select *
from web_events
where duration_sec > 165 and session_id in (10, 17, 34, 36);
