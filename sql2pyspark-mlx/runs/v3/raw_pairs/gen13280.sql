select
    we.id,
    we.event_type,
    we.page,
    s.device,
    s.customer_id
from web_events we
left outer join sessions s on s.id = we.session_id
order by device, customer_id desc;
