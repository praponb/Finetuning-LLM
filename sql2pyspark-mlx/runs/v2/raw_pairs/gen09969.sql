SELECT oi.quantity, oi.order_id, o.amount
FROM order_items oi
RIGHT JOIN orders o ON oi.order_id = o.id
WHERE oi.quantity < 4
ORDER BY oi.quantity ASC;
