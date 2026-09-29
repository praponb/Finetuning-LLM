select oi.order_id, oi.unit_price, o.discount
from order_items oi
join orders o on o.id = oi.order_id
where o.shipping_fee between 15 and 38 and oi.quantity < 4
