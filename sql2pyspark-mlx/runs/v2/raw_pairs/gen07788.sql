select
    we.page,
    we.session_id,
    s.started_at,
    s.device,
    c.country,
    c.email
from web_events we
inner join sessions s on we.session_id = s.id
inner join customers c on s.customer_id = c.id
where c.country = 'US' and to_date(we.event_time) < '2024-04-13'
