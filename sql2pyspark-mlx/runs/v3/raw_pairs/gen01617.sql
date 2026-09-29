select
    oi.id,
    oi.unit_price,
    oi.quantity,
    oi.order_id,
    o.discount,
    o.order_date,
    o.shipping_fee,
    trim(o.channel) as clean_channel
from order_items oi
inner join orders o on o.id = oi.order_id
where o.discount >= 0.12 and year(o.order_date) = 2023
