select
    o.id,
    o.store_id,
    o.channel,
    o.amount
from orders o
where not exists (select 1 from order_items oi where oi.order_id = o.id and oi.quantity < 6);
