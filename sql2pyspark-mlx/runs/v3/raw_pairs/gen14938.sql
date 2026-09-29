select
    a.id,
    a.balance,
    a.status,
    a.customer_id,
    c.is_active
from accounts a
inner join customers c on a.customer_id = c.id
order by customer_id, a.id desc
limit 3;
