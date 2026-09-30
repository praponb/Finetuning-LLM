select sessions.id, sessions.device, sessions.customer_id
from sessions
where not exists (select 1 from web_events where web_events.session_id = sessions.id)
order by id desc, 2 desc
limit 5;
