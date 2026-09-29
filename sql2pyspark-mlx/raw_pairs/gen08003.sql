SELECT
    inventory.last_restock,
    inventory.product_id,
    products.brand,
    stores.sqft,
    ROUND(inventory.quantity, 1) AS quantity_rounded
FROM inventory
RIGHT OUTER JOIN products ON inventory.product_id = products.id
LEFT OUTER JOIN stores ON stores.id = inventory.store_id
