select o.id, o.status
from orders o
where o.id in (select order_id from payments where paid_at >= '2024-07-05');
