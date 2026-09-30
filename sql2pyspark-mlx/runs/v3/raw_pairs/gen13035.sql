select c.id, c.credit_limit
from customers c
where not exists (select 1 from subscriptions s where s.customer_id = c.id and s.end_date < '2023-06-01')
order by credit_limit asc;
