SELECT i.id, i.last_restock, i.quantity
FROM inventory i
WHERE i.store_id IN (SELECT id FROM stores WHERE sqft < 7456)
ORDER BY id
LIMIT 10
