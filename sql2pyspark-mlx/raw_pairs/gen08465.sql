SELECT
    inventory.id,
    inventory.product_id,
    store_sales.sale_date,
    store_sales.revenue,
    DATE_TRUNC('MONTH', store_sales.sale_date) AS sale_date_period
FROM inventory
LEFT OUTER JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id
WHERE MONTH(inventory.last_restock) IN (1, 4, 6);
