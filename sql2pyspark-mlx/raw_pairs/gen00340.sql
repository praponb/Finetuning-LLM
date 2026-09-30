select we.id, we.event_time
from web_events we
where we.event_time >= '2023-09-10' and we.session_id in (select id from sessions where country in ('SG', 'TH', 'DE'));
