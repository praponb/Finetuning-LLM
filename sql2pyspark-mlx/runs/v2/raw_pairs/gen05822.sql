select
    s.device,
    c.country,
    c.email,
    c.is_active,
    to_date(s.started_at) as started_at_date
from sessions s
inner join customers c on s.customer_id = c.id
where c.segment <> 'Consumer' and c.is_active = true;
