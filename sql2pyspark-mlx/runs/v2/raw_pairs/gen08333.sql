select
    p.method,
    p.order_id,
    p.status,
    o.amount,
    o.shipping_fee
from payments p
join orders o on p.order_id = o.id
order by status desc, method;
