select sessions.id, sessions.browser, sessions.device
from sessions
where not exists (select 1 from web_events where web_events.session_id = sessions.id)
order by sessions.id;
