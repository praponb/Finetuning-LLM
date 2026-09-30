select a.id, a.status, a.opened_date
from accounts a
where a.customer_id is not null and a.customer_id not in (select id from customers where segment is null)
order by a.opened_date desc
