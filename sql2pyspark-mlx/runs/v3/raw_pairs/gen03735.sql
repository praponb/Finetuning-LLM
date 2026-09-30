select p.id, p.method, p.paid_at
from payments p
where p.order_id in (select id from orders where store_id is not null)
