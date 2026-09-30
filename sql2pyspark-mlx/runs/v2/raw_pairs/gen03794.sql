SELECT
    inventory.id,
    inventory.product_id,
    inventory.quantity,
    store_sales.product_id AS store_sale_product_id,
    store_sales.revenue
FROM inventory
JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id
WHERE MONTH(store_sales.sale_date) IN (2, 3, 7)
