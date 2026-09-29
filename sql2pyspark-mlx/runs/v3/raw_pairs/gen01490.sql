SELECT oi.product_id, oi.quantity, o.discount
FROM order_items oi
LEFT JOIN orders o ON oi.order_id = o.id
WHERE o.discount BETWEEN 0.13 AND 0.34
ORDER BY quantity DESC;
