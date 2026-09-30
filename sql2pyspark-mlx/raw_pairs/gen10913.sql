SELECT i.id, i.product_id, i.quantity
FROM inventory i
WHERE i.last_restock >= '2024-05-01' AND i.product_id IN (SELECT id FROM products WHERE product_name IS NOT NULL)
ORDER BY i.quantity DESC
