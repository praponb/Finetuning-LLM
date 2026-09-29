select c.id, c.signup_date
from customers c
where c.city = 'London' and exists (select 1 from sessions s where s.customer_id = c.id)
order by id desc
