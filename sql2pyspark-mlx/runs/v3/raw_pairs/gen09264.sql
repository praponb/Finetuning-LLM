SELECT
    inventory.id,
    inventory.quantity,
    inventory.product_id,
    store_sales.sale_date,
    store_sales.units,
    store_sales.product_id AS store_sale_product_id,
    CEIL(store_sales.units) AS units_whole
FROM inventory
INNER JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id
WHERE inventory.quantity IS NOT NULL
ORDER BY product_id DESC;
