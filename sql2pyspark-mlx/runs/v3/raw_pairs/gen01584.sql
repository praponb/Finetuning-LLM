select web_events.id, web_events.duration_sec
from web_events
where web_events.session_id in (1, 12) and web_events.session_id not in (select id from sessions where browser in ('Firefox', 'Edge'));
