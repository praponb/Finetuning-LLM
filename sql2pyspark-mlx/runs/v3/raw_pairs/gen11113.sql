select p.id, p.method, p.order_id
from payments p
where p.order_id in (select id from orders where discount > 0.37)
