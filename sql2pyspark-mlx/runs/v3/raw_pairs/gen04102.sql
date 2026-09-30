SELECT
    inventory.id,
    inventory.store_id,
    store_sales.units,
    store_sales.product_id
FROM inventory
INNER JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id
WHERE inventory.product_id IN (2, 7, 9) AND inventory.product_id IN (1, 4, 6)
ORDER BY store_id;
