select s.id, s.status, s.end_date
from subscriptions s
where s.customer_id in (select id from customers where length(name) < 7);
