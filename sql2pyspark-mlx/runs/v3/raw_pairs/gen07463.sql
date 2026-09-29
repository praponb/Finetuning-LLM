SELECT inventory.product_id, inventory.store_id, stores.store_name, stores.region, stores.opened_date FROM inventory FULL OUTER JOIN stores ON inventory.store_id = stores.id ORDER BY 5 DESC
