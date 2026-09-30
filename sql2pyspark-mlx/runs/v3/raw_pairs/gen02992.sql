SELECT inventory.id, inventory.store_id FROM inventory WHERE inventory.product_id NOT IN (SELECT id FROM products WHERE brand IN ('Initech', 'Globex'));
