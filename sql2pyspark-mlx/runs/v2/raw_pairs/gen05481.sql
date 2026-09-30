SELECT stores.opened_date, inventory.last_restock, inventory.product_id, inventory.store_id FROM stores LEFT JOIN inventory ON inventory.store_id = stores.id;
