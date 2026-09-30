SELECT
    inventory.id,
    inventory.last_restock,
    stores.sqft,
    stores.city,
    stores.store_name
FROM inventory
INNER JOIN stores ON stores.id = inventory.store_id
