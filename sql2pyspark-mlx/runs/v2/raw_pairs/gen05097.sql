select
    oi.order_id,
    oi.product_id,
    o.shipping_fee,
    o.channel,
    o.customer_id,
    round(o.discount, 2) as discount_rounded
from order_items oi
join orders o on o.id = oi.order_id
