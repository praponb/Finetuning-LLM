select a.id, a.customer_id, a.opened_date
from accounts a
where a.customer_id not in (select id from customers where age = 52);
