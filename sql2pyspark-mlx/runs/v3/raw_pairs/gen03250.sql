select oi.id, oi.order_id, oi.quantity, oi.unit_price from order_items oi where oi.order_id in (select id from orders where amount between 530 and 1020) order by oi.id
