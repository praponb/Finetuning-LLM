SELECT oi.id, oi.unit_price, oi.product_id
FROM order_items oi
WHERE oi.product_id IN (SELECT id FROM products WHERE cost BETWEEN 540 AND 570)
ORDER BY product_id;
