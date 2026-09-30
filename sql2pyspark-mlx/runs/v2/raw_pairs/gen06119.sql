select oi.id, oi.product_id, oi.quantity from order_items oi where oi.product_id is not null and oi.product_id in (select id from products where month(launch_date) in (3, 8, 12));
