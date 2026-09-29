select
    o.id,
    o.shipping_fee,
    o.discount,
    o.customer_id
from orders o
where not exists (select 1 from payments p where p.order_id = o.id and p.order_id = 32)
