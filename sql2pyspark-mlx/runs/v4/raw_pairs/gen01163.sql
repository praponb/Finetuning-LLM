select
    c.id,
    c.name,
    c.city,
    c.age
from customers c
where c.id in (select customer_id from orders);
