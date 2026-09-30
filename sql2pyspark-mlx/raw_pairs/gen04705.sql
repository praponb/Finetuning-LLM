select
    we.id,
    we.event_type,
    we.event_time,
    s.device
from web_events we
left outer join sessions s on we.session_id = s.id
where s.country = 'TH';
