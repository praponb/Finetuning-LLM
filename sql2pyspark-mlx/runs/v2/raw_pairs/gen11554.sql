SELECT inventory.id, inventory.quantity, stores.region FROM inventory INNER JOIN stores ON stores.id = inventory.store_id ORDER BY id DESC
