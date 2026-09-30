select we.id, we.event_time
from web_events we
where we.event_time >= '2024-09-01' and we.session_id in (select id from sessions where device = 'desktop')
