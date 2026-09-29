SELECT oi.unit_price, o.store_id, o.channel, o.order_date FROM order_items oi INNER JOIN orders o ON oi.order_id = o.id ORDER BY oi.unit_price, o.channel;
