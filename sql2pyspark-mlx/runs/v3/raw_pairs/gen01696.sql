SELECT
    inventory.id,
    inventory.store_id,
    inventory.last_restock,
    store_sales.sale_date
FROM inventory
LEFT JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id;
