select a.id, a.status
from accounts a
where a.customer_id is null and a.customer_id not in (select id from customers where email like '%@mail.co.th');
