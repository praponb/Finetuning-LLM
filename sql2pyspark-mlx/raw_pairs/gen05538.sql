select
    o.order_date,
    o.customer_id,
    oi.order_id,
    oi.unit_price
from orders o
left join order_items oi on oi.order_id = o.id;
