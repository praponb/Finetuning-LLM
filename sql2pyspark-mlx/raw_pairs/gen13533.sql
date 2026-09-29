select c.id, c.age, c.is_active
from customers c
where exists (select 1 from sessions s where s.customer_id = c.id)
order by id desc, age
