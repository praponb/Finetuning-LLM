select
    p.id,
    p.status,
    p.order_id,
    p.paid_at
from payments p
where p.order_id in (select id from orders where discount < 0.11)
order by status
