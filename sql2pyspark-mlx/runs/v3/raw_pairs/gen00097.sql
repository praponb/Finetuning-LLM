select oi.id, oi.unit_price from order_items oi where oi.quantity = 7 and oi.order_id in (select id from orders where status != 'PENDING');
