SELECT i.id, i.quantity, i.last_restock
FROM inventory i
WHERE i.store_id IN (1, 2, 4, 6) AND i.store_id IN (SELECT id FROM stores WHERE city = 'Tokyo')
