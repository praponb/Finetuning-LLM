SELECT
    store_sales.product_id,
    store_sales.store_id,
    COUNT(inventory.quantity) AS quantity_count,
    MAX(inventory.quantity) AS max_quantity
FROM inventory
LEFT JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id
GROUP BY store_sales.product_id, store_sales.store_id
HAVING max_quantity >= 240
ORDER BY store_sales.product_id DESC
