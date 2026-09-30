select
    s.id,
    s.plan,
    s.status,
    c.is_active
from subscriptions s
inner join customers c on s.customer_id = c.id;
