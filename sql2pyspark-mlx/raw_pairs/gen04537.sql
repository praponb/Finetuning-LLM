select we.id, we.event_type, we.session_id
from web_events we
where we.session_id not in (select id from sessions where country = 'DE')
order by event_type
