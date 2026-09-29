SELECT stores.id, stores.city, stores.region
FROM stores
WHERE NOT EXISTS (SELECT 1 FROM inventory WHERE inventory.store_id = stores.id AND inventory.product_id IN (1, 4, 6, 10))
ORDER BY city ASC, id ASC
LIMIT 10;
