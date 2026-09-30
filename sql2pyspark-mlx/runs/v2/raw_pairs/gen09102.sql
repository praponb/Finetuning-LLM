select we.id, we.duration_sec
from web_events we
where we.event_type = 'page_view' and we.session_id not in (select id from sessions where country = 'DE')
order by duration_sec desc
