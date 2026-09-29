select
    we.id,
    we.session_id,
    s.device,
    s.country,
    s.browser
from web_events we
full outer join sessions s on we.session_id = s.id
where we.event_type not in ('page_view', 'search')
