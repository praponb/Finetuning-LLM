select oi.id, oi.quantity, oi.order_id, o.amount, o.channel from order_items oi join orders o on oi.order_id = o.id order by order_id desc;
