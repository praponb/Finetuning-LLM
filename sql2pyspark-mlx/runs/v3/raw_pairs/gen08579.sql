select
    c.id,
    c.city,
    c.is_active,
    c.age
from customers c
where exists (select 1 from orders o where o.customer_id = c.id)
