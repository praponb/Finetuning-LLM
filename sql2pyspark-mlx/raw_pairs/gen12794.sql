SELECT
    inventory.id,
    inventory.quantity,
    store_sales.revenue,
    store_sales.sale_date,
    store_sales.product_id,
    DATE_ADD(inventory.last_restock, 41) AS last_restock_plus_41d
FROM inventory
LEFT JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id
WHERE inventory.last_restock <= '2023-06-01'
ORDER BY store_sales.product_id ASC;
