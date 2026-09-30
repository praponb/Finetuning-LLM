SELECT oi.id, oi.product_id, oi.quantity
FROM order_items oi
WHERE oi.order_id IN (SELECT id FROM orders WHERE amount BETWEEN 870 AND 900)
