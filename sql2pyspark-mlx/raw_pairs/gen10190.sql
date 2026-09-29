select c.id, c.country, c.segment
from customers c
where not exists (select 1 from accounts a where a.customer_id = c.id and a.balance between 28000 and 67000);
