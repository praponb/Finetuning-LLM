select
    o.store_id,
    o.discount,
    o.status,
    p.status as payment_status
from orders o
left join payments p on p.order_id = o.id;
