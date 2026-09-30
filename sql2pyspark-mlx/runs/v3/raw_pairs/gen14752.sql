select oi.id, oi.unit_price, oi.quantity, oi.product_id from order_items oi where oi.product_id in (select id from products where product_name is not null) order by quantity desc, id;
