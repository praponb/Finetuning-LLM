select oi.id, oi.product_id from order_items oi where oi.order_id in (1, 32) and oi.order_id in (select id from orders where discount <= 0.36) order by id, oi.product_id desc;
