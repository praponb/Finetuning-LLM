SELECT i.id, i.product_id, i.last_restock
FROM inventory i
WHERE i.product_id IN (SELECT id FROM products WHERE price > 720)
ORDER BY i.last_restock, id
LIMIT 20;
