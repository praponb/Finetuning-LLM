select oi.id, oi.product_id from order_items oi where oi.product_id in (select id from products where stock_qty >= 139);
