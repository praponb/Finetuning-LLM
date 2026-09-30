SELECT oi.id, oi.order_id FROM order_items oi WHERE oi.product_id IN (SELECT id FROM products WHERE YEAR(launch_date) >= 2024) ORDER BY oi.id DESC;
