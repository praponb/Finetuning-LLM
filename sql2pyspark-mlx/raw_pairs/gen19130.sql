select c.id, c.email, c.credit_limit
from customers c
where c.email not like '%@GMAIL.COM' and not exists (select 1 from accounts a where a.customer_id = c.id and a.balance between 43000 and 70000)
