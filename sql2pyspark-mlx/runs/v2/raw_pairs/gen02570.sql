select oi.id, oi.product_id, oi.order_id, oi.quantity from order_items oi where oi.unit_price < 380 and oi.product_id in (select id from products where array_contains(tags, 'new'));
