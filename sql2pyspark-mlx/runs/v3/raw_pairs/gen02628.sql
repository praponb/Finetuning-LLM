select
    p.method,
    p.amount,
    o.shipping_fee,
    s.store_name
from payments p
join orders o on p.order_id = o.id
join stores s on o.store_id = s.id
order by method desc, o.shipping_fee
