select c.id, c.city
from customers c
where not exists (select 1 from accounts a where a.customer_id = c.id);
