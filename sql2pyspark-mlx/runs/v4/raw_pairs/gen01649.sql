select p.id, p.method, p.paid_at
from payments p
where p.status in ('REFUNDED', 'FAILED') and p.order_id in (select id from orders where month(order_date) in (1, 4, 9))
order by p.paid_at, id desc
