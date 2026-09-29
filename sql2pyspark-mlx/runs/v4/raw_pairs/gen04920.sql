select
    p.id,
    p.paid_at,
    p.method,
    p.order_id
from payments p
where p.order_id not in (select id from orders where channel in ('store', 'web'));
