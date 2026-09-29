select oi.id, oi.quantity, oi.order_id
from order_items oi
where oi.product_id in (select id from products where cost <= 510)
