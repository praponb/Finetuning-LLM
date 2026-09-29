select
    we.id,
    we.duration_sec,
    we.session_id,
    s.device
from web_events we
left join sessions s on we.session_id = s.id
where we.session_id in (28, 32)
