select
    s.device,
    s.country,
    s.started_at,
    c.segment,
    c.credit_limit
from sessions s
inner join customers c on c.id = s.customer_id
where s.country != 'DE';
