select oi.order_id, oi.unit_price, oi.quantity, o.channel, o.store_id, o.discount from order_items oi inner join orders o on oi.order_id = o.id;
