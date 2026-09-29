select c.id, c.name
from customers c
where c.id in (select customer_id from orders)
order by name, id
limit 5
