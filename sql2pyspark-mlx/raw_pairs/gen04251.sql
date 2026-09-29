select
    we.event_time,
    we.duration_sec,
    s.device,
    hour(s.started_at) as started_at_hour
from web_events we
left outer join sessions s on we.session_id = s.id
order by we.event_time
