SELECT inventory.product_id, inventory.last_restock, stores.region
FROM inventory
INNER JOIN stores ON stores.id = inventory.store_id
WHERE inventory.quantity < 207
