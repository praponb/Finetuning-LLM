SELECT
    inventory.id,
    inventory.store_id,
    inventory.last_restock,
    inventory.product_id,
    store_sales.units,
    store_sales.sale_date,
    store_sales.revenue,
    DATE_FORMAT(store_sales.sale_date, 'yyyy-MM') AS sale_date_label
FROM inventory
INNER JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id
