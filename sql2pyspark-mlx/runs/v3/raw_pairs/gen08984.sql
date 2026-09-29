select sessions.id, sessions.started_at
from sessions
where sessions.id in (select session_id from web_events)
order by sessions.started_at, sessions.id;
