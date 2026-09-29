SELECT oi.id, oi.product_id
FROM order_items oi
WHERE oi.order_id = 22 AND oi.product_id IN (SELECT id FROM products WHERE YEAR(launch_date) = 2023);
