select
    p.id,
    p.paid_at,
    p.method,
    p.amount,
    o.shipping_fee
from payments p
inner join orders o on p.order_id = o.id
order by id asc
limit 1
