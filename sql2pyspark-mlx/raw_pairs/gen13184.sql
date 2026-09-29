SELECT p.id, p.price, p.category
FROM products p
WHERE NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id AND oi.quantity < 8)
ORDER BY category, p.id DESC;
