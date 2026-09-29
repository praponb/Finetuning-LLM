select oi.id, oi.order_id, oi.product_id, oi.quantity from order_items oi where oi.order_id in (select id from orders where status = 'RETURNED');
