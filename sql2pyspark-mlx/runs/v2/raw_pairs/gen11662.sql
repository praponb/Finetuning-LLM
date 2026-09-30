select
    a.balance,
    a.customer_id,
    a.status,
    c.city,
    c.is_active,
    c.email
from accounts a
left join customers c on c.id = a.customer_id;
