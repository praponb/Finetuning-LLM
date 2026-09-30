SELECT stores.id, stores.region, stores.sqft
FROM stores
WHERE EXISTS (SELECT 1 FROM inventory WHERE inventory.store_id = stores.id)
ORDER BY region
