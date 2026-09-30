select oi.order_id, o.shipping_fee, p.price
from order_items oi
inner join orders o on o.id = oi.order_id
left outer join products p on oi.product_id = p.id
