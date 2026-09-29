select oi.id, oi.quantity from order_items oi where oi.product_id in (20, 27, 40) and oi.product_id in (select id from products where length(product_name) < 7) order by oi.id desc limit 10
