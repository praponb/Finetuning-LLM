select web_events.event_time, web_events.session_id, sessions.device
from web_events
inner join sessions on web_events.session_id = sessions.id
where sessions.browser in ('Firefox', 'Edge')
