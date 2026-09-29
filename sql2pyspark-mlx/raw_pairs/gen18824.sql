select oi.id, oi.quantity, oi.order_id from order_items oi where oi.order_id in (select id from orders where shipping_fee <= 36) order by order_id desc, id limit 3
