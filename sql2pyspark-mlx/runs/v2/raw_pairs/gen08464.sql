SELECT oi.id, oi.quantity, oi.unit_price, o.amount FROM order_items oi INNER JOIN orders o ON oi.order_id = o.id
