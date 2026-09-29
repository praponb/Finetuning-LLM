select oi.id, oi.unit_price, oi.product_id from order_items oi where oi.order_id in (select id from orders where shipping_fee is null);
