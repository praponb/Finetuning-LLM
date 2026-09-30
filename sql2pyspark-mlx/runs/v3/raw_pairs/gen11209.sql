SELECT
    inventory.id,
    inventory.store_id,
    inventory.quantity,
    inventory.product_id,
    store_sales.product_id AS store_sale_product_id,
    store_sales.sale_date,
    store_sales.revenue
FROM inventory
LEFT OUTER JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id
WHERE inventory.store_id IN (1, 3)
ORDER BY store_sales.revenue, quantity;
