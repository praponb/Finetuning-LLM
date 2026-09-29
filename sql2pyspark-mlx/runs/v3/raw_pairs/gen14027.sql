SELECT i.id, i.last_restock, i.store_id
FROM inventory i
WHERE i.store_id IN (1, 3) AND i.product_id IN (SELECT id FROM products WHERE cost < 300);
