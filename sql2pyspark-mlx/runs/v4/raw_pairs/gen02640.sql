select sessions.id, sessions.device
from sessions
where sessions.browser is not null and exists (select 1 from web_events where web_events.session_id = sessions.id);
