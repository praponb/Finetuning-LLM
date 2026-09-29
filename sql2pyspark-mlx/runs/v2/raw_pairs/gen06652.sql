SELECT
    inventory.store_id,
    inventory.last_restock,
    inventory.product_id,
    store_sales.revenue,
    store_sales.store_id AS store_sale_store_id
FROM inventory
LEFT JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id
WHERE inventory.store_id IN (1, 2, 3, 5)
