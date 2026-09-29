select
    o.id,
    o.amount,
    o.discount,
    o.customer_id,
    c.email
from orders o
full outer join customers c on o.customer_id = c.id
