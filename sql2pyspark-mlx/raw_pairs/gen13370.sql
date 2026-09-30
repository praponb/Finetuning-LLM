SELECT
    stores.id,
    stores.region,
    stores.sqft,
    stores.store_name
FROM stores
WHERE NOT EXISTS (SELECT 1 FROM inventory WHERE inventory.store_id = stores.id)
ORDER BY id DESC, stores.region DESC;
