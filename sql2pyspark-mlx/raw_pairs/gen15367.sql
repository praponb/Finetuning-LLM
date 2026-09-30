SELECT
    inventory.id,
    inventory.quantity,
    inventory.last_restock,
    products.supplier_id,
    products.category,
    stores.store_name,
    stores.region
FROM inventory
JOIN products ON inventory.product_id = products.id
JOIN stores ON inventory.store_id = stores.id
WHERE inventory.last_restock >= '2025-03-06' AND stores.sqft > 11317;
