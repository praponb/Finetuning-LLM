select
    o.store_id,
    o.order_date,
    o.discount,
    p.amount,
    p.status,
    p.order_id
from orders o
left join payments p on p.order_id = o.id
where p.order_id in (10, 16);
