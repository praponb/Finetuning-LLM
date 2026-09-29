select
    we.id,
    we.event_type,
    s.country,
    s.started_at
from web_events we
left join sessions s on we.session_id = s.id
order by started_at desc;
