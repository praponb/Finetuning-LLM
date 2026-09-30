select
    a.id,
    a.status,
    c.country,
    c.credit_limit,
    c.email
from accounts a
inner join customers c on a.customer_id = c.id;
