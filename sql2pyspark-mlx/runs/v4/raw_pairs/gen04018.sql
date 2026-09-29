SELECT oi.id, oi.order_id, oi.quantity, oi.unit_price, o.status, o.amount, o.discount FROM order_items oi INNER JOIN orders o ON o.id = oi.order_id;
