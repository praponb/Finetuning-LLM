select
    oi.id,
    oi.product_id,
    oi.quantity,
    oi.order_id
from order_items oi
where oi.order_id in (select id from orders where store_id is not null);
