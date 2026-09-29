select s.id, s.country
from sessions s
where s.country = 'TH' and s.customer_id in (select id from customers where age >= 53)
