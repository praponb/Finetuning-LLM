select
    sessions.id,
    sessions.device,
    sessions.browser,
    sessions.started_at
from sessions
where exists (select 1 from web_events where web_events.session_id = sessions.id)
