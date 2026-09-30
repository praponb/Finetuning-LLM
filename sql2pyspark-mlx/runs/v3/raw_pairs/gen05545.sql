SELECT inventory.id, inventory.store_id FROM inventory WHERE inventory.store_id NOT IN (SELECT id FROM stores WHERE region = 'South') ORDER BY id DESC;
