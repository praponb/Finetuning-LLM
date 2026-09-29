select oi.id, oi.unit_price, oi.quantity from order_items oi where oi.order_id not in (select id from orders where shipping_fee > 14) order by oi.id;
