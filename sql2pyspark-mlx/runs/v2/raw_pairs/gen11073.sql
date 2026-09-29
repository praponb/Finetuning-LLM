SELECT inventory.id, inventory.product_id, stores.city
FROM inventory
INNER JOIN stores ON stores.id = inventory.store_id
WHERE inventory.last_restock <= '2025-10-01'
