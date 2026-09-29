SELECT p.id, p.brand, p.cost
FROM products p
WHERE p.price < 950 AND EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id AND oi.order_id IN (17, 38));
