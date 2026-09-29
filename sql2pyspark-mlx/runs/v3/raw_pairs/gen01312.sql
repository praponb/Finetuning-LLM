SELECT inventory.store_id, stores.sqft, stores.store_name
FROM inventory
JOIN stores ON inventory.store_id = stores.id
ORDER BY inventory.store_id DESC;
