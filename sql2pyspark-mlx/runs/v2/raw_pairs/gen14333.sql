SELECT
    inventory.id,
    inventory.last_restock,
    inventory.product_id,
    inventory.quantity,
    store_sales.product_id AS store_sale_product_id,
    store_sales.units,
    store_sales.sale_date
FROM inventory
LEFT JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id
WHERE inventory.store_id = 6 OR inventory.last_restock > '2023-10-25'
