select
    o.shipping_fee,
    o.customer_id,
    oi.product_id,
    oi.quantity
from orders o
inner join order_items oi on oi.order_id = o.id
order by oi.product_id desc, o.customer_id
