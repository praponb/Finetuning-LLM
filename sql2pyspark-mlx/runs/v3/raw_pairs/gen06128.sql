select
    o.shipping_fee,
    o.channel,
    o.amount,
    oi.order_id,
    oi.unit_price,
    oi.quantity
from orders o
inner join order_items oi on oi.order_id = o.id
where oi.quantity < 5;
