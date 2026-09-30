SELECT stores.id, stores.city
FROM stores
WHERE EXISTS (SELECT 1 FROM inventory WHERE inventory.store_id = stores.id AND inventory.store_id IN (1, 6));
