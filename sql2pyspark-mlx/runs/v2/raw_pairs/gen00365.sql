select c.id, c.signup_date
from customers c
where c.id in (select customer_id from sessions where browser = 'Edge')
