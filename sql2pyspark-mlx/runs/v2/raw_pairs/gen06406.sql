select oi.id, oi.unit_price from order_items oi where oi.order_id not in (select id from orders where discount >= 0.23) order by oi.id desc limit 50
