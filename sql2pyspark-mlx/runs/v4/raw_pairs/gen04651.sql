select o.id, o.status, o.customer_id
from orders o
where exists (select 1 from payments p where p.order_id = o.id)
