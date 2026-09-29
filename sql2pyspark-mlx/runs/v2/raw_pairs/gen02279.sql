select c.id, c.city, c.country
from customers c
where exists (select 1 from orders o where o.customer_id = c.id);
