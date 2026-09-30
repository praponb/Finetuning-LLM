select inventory.product_id, inventory.store_id, inventory.quantity, stores.sqft, stores.city from inventory left join stores on stores.id = inventory.store_id
