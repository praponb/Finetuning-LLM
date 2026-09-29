SELECT
    inventory.last_restock,
    store_sales.units,
    store_sales.product_id,
    store_sales.store_id
FROM inventory
LEFT OUTER JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id
WHERE inventory.store_id IN (3, 5)
ORDER BY units DESC;
