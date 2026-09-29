SELECT
    inventory.quantity,
    stores.opened_date,
    products.product_name,
    products.supplier_id,
    ROUND(inventory.quantity, 1) AS quantity_rounded
FROM inventory
LEFT JOIN stores ON inventory.store_id = stores.id
LEFT JOIN products ON inventory.product_id = products.id
