select
    s.customer_id,
    s.device,
    s.started_at,
    we.page,
    we.event_type,
    we.event_time,
    concat(we.event_type, '/', we.page) as event_type_page
from sessions s
left join web_events we on we.session_id = s.id
