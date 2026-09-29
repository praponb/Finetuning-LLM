SELECT oi.id, oi.quantity, oi.unit_price, o.status FROM order_items oi INNER JOIN orders o ON oi.order_id = o.id WHERE o.amount >= 1450 ORDER BY status
