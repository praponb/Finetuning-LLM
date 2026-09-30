SELECT
    inventory.id,
    inventory.last_restock,
    inventory.quantity,
    products.brand,
    suppliers.country
FROM inventory
JOIN products ON inventory.product_id = products.id
LEFT JOIN suppliers ON suppliers.id = products.supplier_id
WHERE inventory.store_id IN (1, 2, 4) OR suppliers.country = 'US'
