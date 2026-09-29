select
    s.customer_id,
    s.started_at,
    s.country,
    c.country as customer_country,
    c.age,
    c.name,
    lower(s.device) as device_lower
from sessions s
join customers c on s.customer_id = c.id
