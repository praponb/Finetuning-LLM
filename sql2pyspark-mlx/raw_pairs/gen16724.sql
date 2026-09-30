SELECT oi.unit_price, oi.quantity, o.amount, o.discount FROM order_items oi INNER JOIN orders o ON oi.order_id = o.id;
