SELECT stores.id, stores.city, stores.sqft
FROM stores
WHERE NOT EXISTS (SELECT 1 FROM inventory WHERE inventory.store_id = stores.id);
