select sessions.id, sessions.started_at
from sessions
where sessions.browser in ('Edge', 'Chrome', 'Safari') and exists (select 1 from web_events where web_events.session_id = sessions.id)
order by started_at asc
