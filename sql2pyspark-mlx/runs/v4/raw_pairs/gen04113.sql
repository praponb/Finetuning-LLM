SELECT oi.id, oi.product_id
FROM order_items oi
WHERE oi.product_id IS NULL AND oi.order_id NOT IN (SELECT id FROM orders WHERE customer_id IS NULL)
ORDER BY oi.product_id DESC;
