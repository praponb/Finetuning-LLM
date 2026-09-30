select
    s.started_at,
    s.customer_id,
    s.country,
    c.name
from sessions s
join customers c on c.id = s.customer_id
where c.email is null;
