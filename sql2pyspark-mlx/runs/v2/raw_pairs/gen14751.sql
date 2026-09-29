select c.id, c.signup_date
from customers c
where not exists (select 1 from orders o where o.customer_id = c.id and o.discount >= 0.11);
