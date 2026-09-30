select we.id, we.event_time
from web_events we
where we.session_id in (9, 15, 31, 37) and we.session_id not in (select id from sessions where browser = 'Edge');
