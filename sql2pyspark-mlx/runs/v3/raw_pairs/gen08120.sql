select
    s.id,
    s.country,
    s.device,
    c.name,
    c.country as customer_country,
    c.is_active
from sessions s
inner join customers c on s.customer_id = c.id
where s.started_at < '2025-03-15' and s.device not in ('mobile', 'tablet', 'desktop')
