SELECT oi.id, oi.product_id
FROM order_items oi
WHERE oi.product_id IN (SELECT id FROM products WHERE product_name NOT LIKE 'Bac%');
