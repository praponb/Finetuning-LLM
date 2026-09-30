select
    we.id,
    we.event_type,
    we.duration_sec,
    s.started_at,
    s.browser,
    c.age,
    c.city
from web_events we
join sessions s on s.id = we.session_id
left join customers c on c.id = s.customer_id
order by started_at, city desc;
