SELECT
    inventory.id,
    inventory.quantity,
    inventory.product_id,
    inventory.last_restock,
    store_sales.store_id,
    store_sales.units,
    store_sales.revenue,
    COALESCE(inventory.quantity, 0) quantity_filled
FROM inventory
LEFT OUTER JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id
