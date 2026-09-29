SELECT
    inventory.id,
    inventory.last_restock,
    store_sales.store_id,
    store_sales.product_id,
    store_sales.revenue
FROM inventory
INNER JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id
WHERE store_sales.units < 26 AND inventory.store_id IN (1, 2, 5)
ORDER BY store_sales.store_id DESC
