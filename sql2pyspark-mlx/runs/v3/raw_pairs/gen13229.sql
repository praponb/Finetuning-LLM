SELECT
    inventory.id,
    inventory.last_restock,
    inventory.product_id,
    store_sales.revenue
FROM inventory
LEFT JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id
WHERE store_sales.store_id IN (1, 6) AND store_sales.units != 39
ORDER BY product_id DESC;
