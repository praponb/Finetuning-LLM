select we.id, we.event_type
from web_events we
where we.session_id in (select id from sessions where device in ('mobile', 'desktop', 'tablet'));
