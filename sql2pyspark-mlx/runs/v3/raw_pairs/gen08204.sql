SELECT p.id, p.cost
FROM products p
WHERE p.price <= 830 AND NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id AND oi.order_id IN (24, 29, 31))
ORDER BY id;
