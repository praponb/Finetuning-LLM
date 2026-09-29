select a.id, a.customer_id
from accounts a
where a.customer_id in (select id from customers where country not in ('TH', 'DE'));
