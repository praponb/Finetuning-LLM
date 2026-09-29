select
    we.id,
    we.session_id,
    s.started_at,
    s.country
from web_events we
left outer join sessions s on we.session_id = s.id
where we.duration_sec >= 195
