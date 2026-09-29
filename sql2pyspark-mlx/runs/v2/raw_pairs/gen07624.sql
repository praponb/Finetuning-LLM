SELECT oi.id, oi.product_id
FROM order_items oi
WHERE oi.product_id IN (SELECT id FROM products WHERE UPPER(TRIM(product_name)) = 'LAPTOP PRO 14');
