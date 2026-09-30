select
    a.id,
    a.status,
    c.email,
    c.country
from accounts a
inner join customers c on a.customer_id = c.id
