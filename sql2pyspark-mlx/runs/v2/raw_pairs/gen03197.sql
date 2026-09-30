SELECT stores.id, stores.sqft, stores.store_name FROM stores WHERE EXISTS (SELECT 1 FROM inventory WHERE inventory.store_id = stores.id AND inventory.quantity > 238)
