SELECT
    inventory.id,
    inventory.quantity,
    inventory.product_id,
    store_sales.revenue,
    store_sales.store_id,
    ROUND(store_sales.units / store_sales.revenue, 2) AS revenue_per_unit
FROM inventory
LEFT JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id;
