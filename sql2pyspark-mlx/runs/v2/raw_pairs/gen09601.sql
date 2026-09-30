select
    p.id,
    p.paid_at,
    p.method,
    p.amount,
    o.shipping_fee,
    o.discount
from payments p
right join orders o on o.id = p.order_id
order by discount desc
