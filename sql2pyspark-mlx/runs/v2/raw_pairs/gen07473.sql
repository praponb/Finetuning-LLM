select c.signup_date, s.customer_id, upper(c.name) as name_upper
from customers c
join sessions s on c.id = s.customer_id
where c.name like '%Olivi%';
