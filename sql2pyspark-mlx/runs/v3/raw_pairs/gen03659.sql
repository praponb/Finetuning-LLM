select
    a.id,
    a.status,
    a.customer_id,
    a.account_type,
    c.city
from accounts a
right join customers c on a.customer_id = c.id
order by status;
