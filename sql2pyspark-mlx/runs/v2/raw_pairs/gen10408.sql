select
    s.status,
    s.start_date,
    c.email,
    c.country,
    c.age
from subscriptions s
left join customers c on s.customer_id = c.id
where c.is_active;
