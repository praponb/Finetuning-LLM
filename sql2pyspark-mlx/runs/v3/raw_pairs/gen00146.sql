select sessions.id, sessions.customer_id
from sessions
where not exists (select 1 from web_events where web_events.session_id = sessions.id and web_events.page in ('/home', '/checkout', '/cart'))
